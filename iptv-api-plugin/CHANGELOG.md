# Changelog

## v1.0.3 (2026-06-09)

### Changed
- Schedule setting changed from interval hours (number) to specific times
  (string, HHMM format). Example: `0600,1300,1800` runs daily at 6 AM, 1 PM,
  and 6 PM. Uses Celery Beat `CrontabSchedule` instead of `IntervalSchedule`.
- Removed quality-based stream re-sorting. Plugin now preserves the M3U's
  original stream order (iptv-api already sorts by measured speed).
- Removed unused `from django.db.models import Max` import.
- Added logo, README, LICENSE, CHANGELOG for release readiness.

## v1.0.2 (2026-06-09)

### Fixed
- Channel lookup now uses normalized key (`stream_key`) while creation uses
  the original display name (`stream_list[0].name`), fixing the bug where
  streams like `CCTV-1` would create a new channel instead of matching
  existing `CCTV1`.

### Changed
- Lock mechanism upgraded to UUID-based tokens with ownership verification.
- Cleanup and dry_run now wrapped in `transaction.atomic()`.

## v1.0.1 (2026-06-08)

### Added
- Stream name normalization with fuzzy matching: strips hyphens, middle dots,
  quality suffixes (HD, 4K, 高清, etc.) for case-insensitive matching.
- UUID-based operation lock with TTL and stale lock recovery.

### Fixed
- Lock race condition between concurrent runs.
- Scheduled dry_run bypassing transaction rollback.
- Orphan stream cleanup no longer deletes overflowed streams.

## v1.0.0 (2026-06-08)

### Initial release
- Aggregate iptv-api M3U streams into same-name channels.
- Quality-based stream sorting (resolution → bitrate → codec → fps).
- Stale stream cleanup.
- Orphan stream cleanup.
- Dry-run mode for previewing changes.
- Celery Beat scheduling.
- Channel profile membership support.
