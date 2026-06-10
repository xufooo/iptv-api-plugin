"""
iptv-api-plugin — Dispatcharr Plugin

Aggregates iptv-api M3U streams into same-name channels and cleans stale sources.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import timedelta

from celery import shared_task
from django.utils import timezone
from django.db import transaction

PLUGIN_KEY = "iptv-api-plugin"
SCHEDULED_TASK_PATH = "iptv_api_plugin.scheduled_run"
SCHEDULE_TASK_NAME_PREFIX = f"{PLUGIN_KEY} scheduled run"


@shared_task(name=SCHEDULED_TASK_PATH)
def scheduled_run():
    """Celery entry point used by django-celery-beat schedules."""
    from apps.plugins.loader import PluginManager

    return PluginManager.get().run_action(PLUGIN_KEY, "scheduled_run", {})

# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

LOCK_FILE = "/tmp/iptv-api-plugin.lock"
LOCK_TTL_SECONDS = 3600  # 1 hour


def _acquire_lock() -> str | None:
    """Try to acquire a file-based operation lock using atomic mkdir.

    mkdir is atomic on POSIX — only one process succeeds.
    Also handles legacy regular-file locks.
    Returns a lock token on success, or None if lock not acquired.
    """
    lock_path = LOCK_FILE
    now = time.time()
    token = str(uuid.uuid4())

    # Handle legacy regular-file lock from older versions
    if os.path.isfile(lock_path):
        try:
            os.remove(lock_path)
        except OSError:
            pass

    try:
        os.makedirs(lock_path, exist_ok=False)
    except FileExistsError:
        # Lock directory exists — check if it's alive
        lock_data_path = os.path.join(lock_path, "lock.json")
        if os.path.isfile(lock_data_path):
            try:
                with open(lock_data_path, "r") as f:
                    data = json.load(f)
                expires = data.get("expires_at", 0)
                if now < expires:
                    return None  # Still valid
            except (FileNotFoundError, json.JSONDecodeError):
                pass
        else:
            # Directory exists but no metadata — likely another process
            # is still creating it. Wait briefly, then check again.
            time.sleep(0.5)
            if os.path.isfile(lock_data_path):
                try:
                    with open(lock_data_path, "r") as f:
                        data = json.load(f)
                    expires = data.get("expires_at", 0)
                    if now < expires:
                        return None
                except (FileNotFoundError, json.JSONDecodeError):
                    pass

        # Lock is expired or corrupted — take over
        import shutil
        shutil.rmtree(lock_path, ignore_errors=True)
        try:
            os.makedirs(lock_path, exist_ok=False)
        except FileExistsError:
            return None

    # Write metadata with our token
    try:
        with open(os.path.join(lock_path, "lock.json"), "w") as f:
            json.dump({
                "pid": os.getpid(),
                "token": token,
                "started_at": now,
                "expires_at": now + LOCK_TTL_SECONDS,
            }, f)
    except Exception:
        import shutil
        shutil.rmtree(lock_path, ignore_errors=True)
        return None
    return token


def _release_lock(token: str):
    """Release the operation lock. Only removes if we own it (token matches)."""
    import shutil
    try:
        if not os.path.isdir(LOCK_FILE):
            return
        lock_data_path = os.path.join(LOCK_FILE, "lock.json")
        if os.path.isfile(lock_data_path):
            try:
                with open(lock_data_path, "r") as f:
                    data = json.load(f)
                if data.get("token") != token:
                    return  # Not our lock
            except (FileNotFoundError, json.JSONDecodeError):
                return  # Can't verify ownership — don't delete
        else:
            return  # No metadata — don't delete unknown lock
        shutil.rmtree(LOCK_FILE, ignore_errors=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Quality / stream helpers
# ---------------------------------------------------------------------------

_CODEC_RANK = {
    "hevc": 4, "h265": 4, "x265": 4,
    "h264": 3, "x264": 3, "avc": 3,
    "mpeg4": 2, "mp4v": 2,
    "mpeg2": 1, "mp2v": 1,
}


def _parse_int(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _parse_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _stream_quality_score(stream):
    """
    Return a sortable tuple (higher = better).
    Uses stream_stats JSON, falls back to last_seen.
    """
    raw_stats = stream.stream_stats
    stats = raw_stats if isinstance(raw_stats, dict) else {}
    height = _parse_int(stats.get("height") or stats.get("resolution_height"))
    if not height:
        res_raw = stats.get("resolution", "")
        if "x" in str(res_raw):
            parts = str(res_raw).split("x")
            height = _parse_int(parts[-1])
        elif "p" in str(res_raw):
            height = _parse_int(res_raw.replace("p", ""))

    bitrate = _parse_int(stats.get("bitrate") or stats.get("video_bitrate"))
    fps = _parse_float(stats.get("fps") or stats.get("frame_rate"))
    codec = (stats.get("video_codec") or stats.get("codec") or "").lower().strip()
    codec_score = max(_CODEC_RANK.get(c, 0) for c in [codec]) if codec else 0

    last_seen_ts = stream.last_seen.timestamp() if stream.last_seen else 0

    return (height, bitrate, codec_score, fps, last_seen_ts)


# Common quality suffixes to strip from the end (case-insensitive)
_QUALITY_SUFFIX_RE = re.compile(
    r'\s+(?:HD|SD|4K|UHD|FHD|QHD|标清|高清|超清|蓝光|HEVC|HDR|SDR)\s*$',
    re.IGNORECASE
)


def _normalize_name(name: str) -> str:
    """Normalize stream/channel names for fuzzy matching.

    Strips common punctuation (hyphens, dots, middle dots) and quality suffixes
    so that 'CCTV-1 HD' and 'CCTV1' resolve to the same key.
    """
    if not name:
        return ""
    name = name.strip()
    # Strip common quality suffixes (HD, 4K, 高清, etc.)
    name = _QUALITY_SUFFIX_RE.sub('', name).strip()
    # Strip hyphens, middle dots, underscores used in channel names
    # "CCTV-1" → "CCTV1", "BBC·World" → "BBCWorld"
    name = re.sub(r'[\-_\u00b7\u30fb·]+', '', name)
    # Collapse whitespace and lowercase for case-insensitive matching
    return ' '.join(name.split()).lower()


# ---------------------------------------------------------------------------
# Plugin class
# ---------------------------------------------------------------------------


class Plugin:
    """iptv-api-plugin — aggregate streams into channels, clean stale."""

    name = "iptv-api-plugin"
    version = "1.0.4"
    description = "Aggregate iptv-api M3U streams into same-name channels and clean stale sources."
    author = "Dispatcharr Community"
    help_url = "https://github.com/Guovin/iptv-api"

    # Static manifest — fields are rebuilt dynamically below
    fields = []
    actions = [
        {
            "id": "run_now",
            "label": "Run Now",
            "description": "Aggregate streams into channels and apply cleanup.",
            "button_label": "Run Now",
            "button_variant": "filled",
            "button_color": "blue",
            "confirm": {
                "required": True,
                "title": "Run iptv-api aggregation?",
                "message": "This creates channels, changes stream mappings, and may delete stale streams.",
            },
        },
        {
            "id": "preview",
            "label": "Preview",
            "description": "Dry-run aggregation regardless of Dry Run Mode setting.",
            "button_label": "Preview",
            "button_variant": "outline",
            "button_color": "gray",
        },
        {
            "id": "cleanup_now",
            "label": "Cleanup Now",
            "description": "Run cleanup rules without rebuilding channel mappings.",
            "button_label": "Cleanup Now",
            "button_variant": "outline",
            "button_color": "orange",
            "confirm": {
                "required": True,
                "title": "Cleanup streams?",
                "message": "Stale and orphan streams may be deleted.",
            },
        },
        {
            "id": "sync_schedule",
            "label": "Sync Schedule",
            "description": "Create or update Celery Beat periodic schedule.",
            "button_label": "Sync Schedule",
            "button_variant": "outline",
            "button_color": "green",
        },
    ]

    def __init__(self):
        self.fields = self._build_fields()

    # ------------------------------------------------------------------
    # Field construction
    # ------------------------------------------------------------------

    def _build_fields(self):
        """Dynamically build fields, loading ChannelProfile list at runtime."""
        channel_profile_field = {
            "id": "channel_profile",
            "label": "Channel Profile",
            "type": "select",
            "default": "",
            "options": [],
            "help_text": "Channels will be added to this profile. Leave empty to skip.",
        }
        try:
            from apps.channels.models import ChannelProfile as CP

            profiles = CP.objects.all().order_by("name")
            opts = [{"value": "", "label": "--- Do not change profile ---"}]
            opts.extend(
                {"value": str(p.id), "label": p.name} for p in profiles
            )
            channel_profile_field["options"] = opts
        except Exception:
            channel_profile_field["type"] = "string"
            channel_profile_field[
                "help_text"
            ] = "Could not load profiles. Enter profile ID or leave empty."

        return [
            channel_profile_field,
            {
                "id": "channel_group",
                "label": "Channel Group",
                "type": "string",
                "default": "iptv-api",
                "help_text": "Channel group name used when creating channels.",
            },
            {
                "id": "max_streams_per_channel",
                "label": "Max Streams Per Channel",
                "type": "number",
                "default": 3,
                "help_text": "Max streams retained and linked per channel.",
            },
            {
                "id": "cleanup_stale_streams",
                "label": "Cleanup Stale Streams",
                "type": "boolean",
                "default": True,
                "help_text": "Delete is_stale streams after aggregation.",
            },
            {
                "id": "cleanup_orphan_streams",
                "label": "Cleanup Orphan Streams",
                "type": "boolean",
                "default": False,
                "help_text": "Delete streams not linked to any channel. 1h protection window applies.",
            },
            {
                "id": "schedule_times",
                "label": "Schedule Times (24-hour)",
                "type": "string",
                "default": "0600",
                "placeholder": "0600,1300,1800",
                "help_text": "Comma-separated times to run daily. Format HHMM, e.g. 0600,1300,1800. Leave empty to disable.",
            },
            {
                "id": "dry_run_mode",
                "label": "Dry Run Mode",
                "type": "boolean",
                "default": False,
                "help_text": "Preview changes without writing to database.",
            },
        ]

    # ------------------------------------------------------------------
    # Setting normalization
    # ------------------------------------------------------------------

    def _normalize_settings(self, raw):
        """Parse saved settings and return clean dict."""
        if raw is None:
            raw = {}
        return {
            "channel_profile": str(raw.get("channel_profile", "") or ""),
            "channel_group": (str(raw.get("channel_group", "iptv-api")) or "iptv-api").strip(),
            "max_streams_per_channel": max(1, int(raw.get("max_streams_per_channel", 3) or 3)),
            "cleanup_stale_streams": str(raw.get("cleanup_stale_streams", "true")).lower()
            in ("true", "yes", "1"),
            "cleanup_orphan_streams": str(raw.get("cleanup_orphan_streams", "false")).lower()
            in ("true", "yes", "1"),
            "schedule_times": (str(raw.get("schedule_times", "0600") or "")).strip(),
            "dry_run_mode": str(raw.get("dry_run_mode", "false")).lower() in ("true", "yes", "1"),
        }

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self, action: str, params: dict, context: dict):
        logger = context.get("logger")
        try:
            settings = self._normalize_settings(context.get("settings", {}))
        except Exception as e:
            logger.error(f"[iptv-api-plugin] Failed to parse settings: {e}")
            return {"status": "error", "message": f"Invalid settings: {e}"}

        if action == "run_now":
            return self._run_pipeline(settings, logger, dry_run=settings["dry_run_mode"])
        if action == "preview":
            return self._run_pipeline(settings, logger, dry_run=True)
        if action == "cleanup_now":
            return self._cleanup_only(settings, logger, dry_run=settings["dry_run_mode"])
        if action == "scheduled_run":
            return self._run_pipeline(settings, logger, dry_run=settings["dry_run_mode"], scheduled=True)
        if action == "sync_schedule":
            if settings["dry_run_mode"]:
                return {"status": "ok", "message": f"[DRY RUN] Would create CrontabSchedule for times: {settings['schedule_times']}."}
            return self._sync_schedule(settings, logger)

        return {"status": "error", "message": f"Unknown action: {action}"}

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    def _run_pipeline(self, settings, logger, dry_run=False, scheduled=False):
        """Full pipeline: aggregate + cleanup."""
        from apps.channels.models import (
            Channel,
            ChannelGroup,
            ChannelProfile,
            ChannelProfileMembership,
            ChannelStream,
            Stream,
        )

        lock_token = _acquire_lock()
        if lock_token is None:
            return {"status": "error", "message": "Another run is already in progress (lock held)."}

        stats = {
            "dry_run": dry_run,
            "streams_seen": 0,
            "stream_names": 0,
            "channels_created": 0,
            "channels_reused": 0,
            "channel_streams_created": 0,
            "channel_streams_removed": 0,
            "stale_streams_deleted": 0,
            "overflow_streams_skipped": 0,
            "orphan_streams_deleted": 0,
            "custom_streams_skipped": 0,
        }

        try:
            with transaction.atomic():
                # --- group ---
                group_name = settings["channel_group"]
                group = ChannelGroup.objects.filter(name=group_name).first()
                if not group and not dry_run:
                    group = ChannelGroup.objects.create(name=group_name)
                if group:
                    stats["group_name"] = group.name
                else:
                    stats["group_name"] = group_name

                # --- fetch non-stale streams ---
                streams_qs = (
                    Stream.objects.filter(is_stale=False)
                    .exclude(name__isnull=True)
                    .exclude(name__exact="")
                    .select_related("channel_group", "m3u_account")
                    .order_by("name", "-last_seen")
                )
                all_streams = list(streams_qs)
                stats["streams_seen"] = len(all_streams)

                # --- group by name ---
                grouped = {}
                for s in all_streams:
                    key = _normalize_name(s.name)
                    if not key:
                        continue
                    grouped.setdefault(key, []).append(s)

                stats["stream_names"] = len(grouped)

                # --- pre-load existing channels for lookup ---
                name_to_channel = {}
                for ch in Channel.objects.select_related("channel_group").all():
                    nk = _normalize_name(ch.name)
                    if nk:
                        name_to_channel.setdefault(nk, []).append(ch)

                # --- process each group ---
                for stream_key, stream_list in sorted(grouped.items()):
                    # stream_key is normalized (e.g. "cctv1") — used for lookup
                    # stream_list[0].name is original (e.g. "CCTV-1") — used for display
                    # Keep M3U's original order (iptv-api already sorts by speed)
                    selected = stream_list[: settings["max_streams_per_channel"]]
                    overflow = stream_list[settings["max_streams_per_channel"] :]

                    # find or create channel
                    channel = self._find_channel(
                        name_to_channel, stream_key, group, logger
                    )
                    if channel is None and not dry_run:
                        channel = Channel.objects.create(
                            name=stream_list[0].name,  # preserve original display name
                            channel_group=group,
                            channel_number=Channel.get_next_available_channel_number(),
                        )
                        stats["channels_created"] += 1
                    elif channel is None:
                        stats["channels_created"] += 1  # would create
                        stats["channel_streams_created"] += len(selected)  # would create these links
                        if dry_run:
                            logger.info(
                                f"[iptv-api-plugin] [DRY RUN] Would create channel "
                                f"for stream_key='{stream_key}' using name='{stream_list[0].name}' "
                                f"(from {len(stream_list)} streams)"
                            )
                    else:
                        stats["channels_reused"] += 1

                    # attach selected streams
                    if channel and channel.id:
                        self._sync_channel_streams(
                            channel, selected, dry_run, stats, logger
                        )

                    # overflow cleanup — skip excess streams but don't delete
                    # (they might be linked to manually curated channels)
                    overflow_count = len([s for s in overflow if not s.is_custom])
                    stats["overflow_streams_skipped"] += overflow_count

                    # profile membership
                    if channel and channel.id and settings["channel_profile"]:
                        self._ensure_profile_membership(
                            channel, settings["channel_profile"], dry_run, logger
                        )

                # --- channel stream removal (orphan channel-stream links) ---
                # Already handled by _sync_channel_streams which replaces existing links

                # --- cleanup stale streams ---
                if settings["cleanup_stale_streams"]:
                    stale_qs = Stream.objects.filter(is_stale=True, is_custom=False)
                    stale_count = stale_qs.count()
                    stats["stale_streams_deleted"] = stale_count
                    if not dry_run:
                        stale_qs.delete()
                    logger.info(
                        f"[iptv-api-plugin] Stale streams to delete: {stale_count}"
                    )

                # --- cleanup orphan streams ---
                if settings["cleanup_orphan_streams"]:
                    cutoff = timezone.now() - timedelta(hours=1)
                    linked_ids = ChannelStream.objects.values_list("stream_id", flat=True)
                    # Exclude streams that were just unlinked (overflow) from orphan cleanup
                    orphan_qs = (
                        Stream.objects.filter(is_custom=False, last_seen__lt=cutoff)
                        .exclude(id__in=list(linked_ids))
                    )
                    # Also protect streams that were recently part of this plugin's channels
                    # (they may have been overflowed and should not be deleted)
                    if "recently_unlinked" in stats:
                        orphan_qs = orphan_qs.exclude(
                            id__in=stats["recently_unlinked"]
                        )
                    orphan_count = orphan_qs.count()
                    stats["orphan_streams_deleted"] = orphan_count
                    if not dry_run:
                        orphan_qs.delete()
                    logger.info(
                        f"[iptv-api-plugin] Orphan streams to delete: {orphan_count}"
                    )

                if dry_run:
                    transaction.set_rollback(True)

            msg = self._build_summary(stats, dry_run)
            # Clean up non-serializable tracking data before returning
            stats.pop("recently_unlinked", None)
            stats.pop("group_name", None)
            logger.info(f"[iptv-api-plugin] {'Dry run' if dry_run else 'Run'} complete: {msg}")
            return {"status": "ok", "message": msg, "stats": stats}

        except Exception as e:
            logger.error(f"[iptv-api-plugin] Pipeline error: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            _release_lock(lock_token)

    # ------------------------------------------------------------------
    # Cleanup only
    # ------------------------------------------------------------------

    def _cleanup_only(self, settings, logger, dry_run=False):
        """Only run cleanup rules (stale + orphan), no channel aggregation."""
        from apps.channels.models import ChannelStream, Stream

        lock_token = _acquire_lock()
        if lock_token is None:
            return {"status": "error", "message": "Another run is already in progress (lock held)."}

        stats = {"dry_run": dry_run, "stale_streams_deleted": 0, "orphan_streams_deleted": 0}

        try:
            with transaction.atomic():
                if settings["cleanup_stale_streams"]:
                    stale_qs = Stream.objects.filter(is_stale=True, is_custom=False)
                    stats["stale_streams_deleted"] = stale_qs.count()
                    if not dry_run:
                        stale_qs.delete()
                    logger.info(f"[iptv-api-plugin] Stale to delete: {stats['stale_streams_deleted']}")

                if settings["cleanup_orphan_streams"]:
                    cutoff = timezone.now() - timedelta(hours=1)
                    linked_ids = ChannelStream.objects.values_list("stream_id", flat=True)
                    orphan_qs = (
                        Stream.objects.filter(is_custom=False, last_seen__lt=cutoff)
                        .exclude(id__in=list(linked_ids))
                    )
                    stats["orphan_streams_deleted"] = orphan_qs.count()
                    if not dry_run:
                        orphan_qs.delete()
                    logger.info(f"[iptv-api-plugin] Orphan to delete: {stats['orphan_streams_deleted']}")

                if dry_run:
                    transaction.set_rollback(True)

            msg = f"Stale: {stats['stale_streams_deleted']}, Orphan: {stats['orphan_streams_deleted']}"
            if dry_run:
                msg = f"[DRY RUN] {msg}"
            return {"status": "ok", "message": msg, "stats": stats}

        except Exception as e:
            logger.error(f"[iptv-api-plugin] Cleanup error: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            _release_lock(lock_token)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_channel(self, name_to_channel, stream_name, preferred_group, logger):
        """
        Find existing channel matching stream_name.
        Prefers channels in the same group, then any channel with the same name.
        """
        candidates = name_to_channel.get(stream_name, [])
        if not candidates:
            return None
        # prefer same group
        for ch in candidates:
            if ch.channel_group_id and preferred_group and ch.channel_group_id == preferred_group.id:
                return ch
        # fallback: any channel with same name
        return candidates[0]

    def _sync_channel_streams(self, channel, streams, dry_run, stats, logger):
        """
        Replace the channel's stream links with the provided sorted list.
        """
        from apps.channels.models import ChannelStream

        desired_ids = [s.id for s in streams]
        existing_qs = ChannelStream.objects.filter(channel=channel)
        existing = {cs.stream_id: cs for cs in existing_qs}

        # remove links not in desired set
        to_remove = [cs for sid, cs in existing.items() if sid not in desired_ids]
        stats["channel_streams_removed"] += len(to_remove)
        # Track recently unlinked stream IDs to protect from orphan cleanup
        recently_unlinked = stats.setdefault("recently_unlinked", set())
        recently_unlinked.update(cs.stream_id for cs in to_remove)
        if not dry_run and to_remove:
            ids_to_remove = [cs.id for cs in to_remove]
            ChannelStream.objects.filter(id__in=ids_to_remove).delete()

        # create/update links
        for order, stream in enumerate(streams):
            cs = existing.get(stream.id)
            if cs is None:
                if not dry_run:
                    ChannelStream.objects.create(
                        channel=channel,
                        stream=stream,
                        order=order,
                    )
                stats["channel_streams_created"] += 1
            elif cs.order != order:
                if not dry_run:
                    cs.order = order
                    cs.save(update_fields=["order"])
                stats["channel_streams_created"] += 1

    def _ensure_profile_membership(self, channel, profile_id_str, dry_run, logger):
        """Add channel to the configured ChannelProfile."""
        from apps.channels.models import ChannelProfile, ChannelProfileMembership

        try:
            profile_id = int(profile_id_str)
            profile = ChannelProfile.objects.get(id=profile_id)
            if dry_run:
                return  # Just verify profile exists, don't write
            _, created = ChannelProfileMembership.objects.get_or_create(
                channel_profile=profile, channel=channel
            )
            if created:
                logger.info(
                    f"[iptv-api-plugin] Added channel '{channel.name}' to profile '{profile.name}'"
                )
        except (ValueError, ChannelProfile.DoesNotExist):
            logger.warning(
                f"[iptv-api-plugin] ChannelProfile id='{profile_id_str}' not found — skipping."
            )

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    def _sync_schedule(self, settings, logger):
        """Create/update/disable Celery Beat periodic task for specific times.

        Each HHMM value gets its own PeriodicTask. A single crontab with comma-
        separated hours and minutes would run every hour/minute combination.
        """
        schedule_times_str = settings["schedule_times"]

        # Handle empty string — disable
        if not schedule_times_str:
            try:
                from django_celery_beat.models import PeriodicTask

                PeriodicTask.objects.filter(
                    name__startswith=SCHEDULE_TASK_NAME_PREFIX
                ).update(enabled=False)
            except ImportError:
                pass
            return {"status": "ok", "message": "Schedule disabled (times empty)."}

        try:
            from django_celery_beat.models import CrontabSchedule, PeriodicTask
        except ImportError:
            return {
                "status": "error",
                "message": "django_celery_beat not available — cannot create schedule.",
            }

        # Parse HHMM times into distinct hour/minute pairs.
        schedule_times = []
        invalid_parts = []
        seen = set()
        for part in schedule_times_str.split(","):
            part = part.strip()
            if len(part) == 4 and part.isdigit():
                h, m = int(part[:2]), int(part[2:])
                if 0 <= h < 24 and 0 <= m < 60:
                    key = f"{h:02d}{m:02d}"
                    if key not in seen:
                        seen.add(key)
                        schedule_times.append((key, str(h), str(m)))
                    continue
            invalid_parts.append(part or "<empty>")

        if invalid_parts or not schedule_times:
            return {
                "status": "error",
                "message": f"Invalid schedule times: '{schedule_times_str}'. Use HHMM format, e.g. 0600,1300,1800.",
            }

        active_names = []
        for time_key, hour, minute in schedule_times:
            crontab_kwargs = {
                "minute": minute,
                "hour": hour,
                "day_of_week": "*",
                "day_of_month": "*",
                "month_of_year": "*",
            }
            try:
                from core.models import CoreSettings

                crontab_kwargs["timezone"] = CoreSettings.get_system_time_zone()
            except Exception:
                pass

            try:
                crontab, _ = CrontabSchedule.objects.get_or_create(**crontab_kwargs)
            except TypeError:
                crontab_kwargs.pop("timezone", None)
                crontab, _ = CrontabSchedule.objects.get_or_create(**crontab_kwargs)

            task_name = f"{SCHEDULE_TASK_NAME_PREFIX} {time_key}"
            active_names.append(task_name)
            PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    "crontab": crontab,
                    "interval": None,
                    "task": SCHEDULED_TASK_PATH,
                    "args": json.dumps([]),
                    "kwargs": json.dumps({}),
                    "enabled": True,
                },
            )

        PeriodicTask.objects.filter(
            name__startswith=SCHEDULE_TASK_NAME_PREFIX
        ).exclude(name__in=active_names).update(enabled=False)

        return {
            "status": "ok",
            "message": f"Schedule set to run daily at: {', '.join(f'{int(h):02d}:{int(m):02d}' for _, h, m in schedule_times)}",
        }

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    @staticmethod
    def _build_summary(stats, dry_run):
        prefix = "[DRY RUN] " if dry_run else ""
        return (
            f"{prefix}"
            f"{stats['stream_names']} channel names from {stats['streams_seen']} streams — "
            f"{stats['channels_created']} created, {stats['channels_reused']} reused — "
            f"{stats['channel_streams_created']} links added, {stats['channel_streams_removed']} removed — "
            f"stale: {stats['stale_streams_deleted']}, overflow_skipped: {stats['overflow_streams_skipped']}, "
            f"orphan: {stats['orphan_streams_deleted']}"
        )
