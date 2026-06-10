# iptv-api-plugin

[![Dispatcharr plugin](https://img.shields.io/badge/Dispatcharr-plugin-8A2BE2)](https://github.com/Dispatcharr/Dispatcharr)
![Version](https://img.shields.io/badge/version-1.0.4-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) plugin that aggregates
[iptv-api](https://github.com/Guovin/iptv-api) M3U streams into same-name channels
and cleans stale sources.

![logo](logo.png)

## Features

- **Aggregate** — Groups streams with the same or similar name (e.g. `CCTV-1 HD`
  and `CCTV1` are merged into one channel)
- **Fuzzy matching** — Strips hyphens, dots, quality suffixes (HD, 4K, 高清, etc.)
  and normalizes case so `CCTV-1` matches `CCTV1`
- **Preserves M3U order** — Keeps the original stream order from iptv-api, which
  is already sorted by measured speed
- **Dry-run mode** — Preview changes without writing to the database
- **Cleanup** — Removes stale and orphan streams automatically
- **Scheduling** — Periodic runs via Celery Beat
- **Profile support** — Optionally add channels to a Channel Profile

## Installation

1. Download the latest `iptv-api-plugin.zip` from [Releases](https://github.com/your-repo/iptv-api-plugin/releases)
2. In Dispatcharr, go to **Settings → Plugins → Import Plugin**
3. Upload the zip file
4. **Restart the Dispatcharr container** to clear Python module cache
5. Verify the plugin version shows **1.0.4** in the plugin list

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Channel Profile | *(empty)* | Assign channels to a profile. Leave empty to skip. |
| Channel Group | `iptv-api` | Group name for created channels. |
| Max Streams Per Channel | `3` | How many backup streams to keep per channel. |
| Cleanup Stale Streams | `true` | Delete `is_stale` streams after run. |
| Cleanup Orphan Streams | `false` | Delete streams not linked to any channel. |
| Schedule Times | `0600` | Daily scheduled times (HHMM, comma-separated). Empty = disable. |
| Dry Run Mode | `false` | Preview only — no database writes. |

## Actions

| Action | Description |
|--------|-------------|
| **Run Now** | Aggregate streams → channels, apply cleanup. |
| **Preview** | Dry-run aggregation regardless of Dry Run Mode setting. |
| **Cleanup Now** | Run cleanup rules without rebuilding channel mappings. |
| **Sync Schedule** | Create/update Celery Beat periodic schedule. |

## How it works

1. **iptv-api** generates a speed-tested, sorted M3U file
2. Dispatcharr imports the M3U, creating `Stream` records
3. This plugin groups streams by normalized name and creates/updates `Channel` records
4. Each channel gets the top N streams, preserving iptv-api's speed-based order
5. Stale and orphan streams are cleaned up

The plugin preserves iptv-api's speed-tested order rather than re-sorting by
quality. For quality-based sorting, run
[IPTV Checker](https://github.com/PiratesIRC/Dispatcharr-IPTV-Checker-Plugin)
to probe stream metadata — this plugin will then respect the freshest probe data.

## License

MIT
