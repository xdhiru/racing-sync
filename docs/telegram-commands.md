# Telegram command reference

Everything the operator can type or tap in the Telegram chat. The bot is
single-operator: sheets are bound to whoever opened them, and destructive
buttons are only answered for that operator.

All commands accept an optional `@bot` suffix in group chats
(`/active@my_bot`). The `<n>` placeholders below are **group numbers as
shown in the Active Tasks card**; `<hash>` is a tracked infohash (a 10-char
prefix is enough for tracked rows, a full 40-char hash is required where
the row is not in the list).

## Commands shown in the `/` menu

| Command | What it does |
|---|---|
| `/add` | Ingest the `.torrent` file you replied to (or sent with `/add` as the caption) as a Telegram-origin drop, processed like a watch-dir file. |
| `/cancel_match <text>` | Bulk-cancel every live group whose title contains `<text>` (case-insensitive). |
| `/cancel_stalled` | Bulk-cancel every group whose download is stalled (downloading, no seeders, incomplete). |
| `/cancel_status` | Cancel a whole lane: the sheet lists DL / MV / RD / QD with live counts. |
| `/pause_intake` | Pause intake — no new downloads start; in-flight rows drain normally. |
| `/resume_intake` | Resume intake of new downloads. |
| `/tick` | Run one full pipeline tick now (poll + schedule + janitor). |
| `/active` | Send a fresh Active Tasks card now (recovery when the long-lived card was deleted). |
| `/stats` | Pipeline snapshot: rows by state, workers, intake, ignored count, storage footer. |
| `/find <text>` | Read-only Prowlarr search over the download-target indexers (titles + sizes, nothing queued). |
| `/files_<n\|hash>` | Read-only file listing for one torrent/group. |

## Typed commands not in the menu

These need a group number or a hash, so they stay unlisted to keep the
menu short.

| Command | What it does |
|---|---|
| `/cancel_<hash>` | Cancel one release (asks the remember + keep/delete questions). |
| `/cancel_<n>` | Cancel the group shown as number `n` (asks the same questions). |
| `/now_<hash>` | Start now, instead of waiting: a WAITING_INDEXER row uses its starting torrent instead of waiting out Prowlarr; a NEW grace-held row starts its SSD download at once. |
| `/fetch_<hash>` | Legacy alias of `/now_`. |
| `/prefer_<hash>` | Legacy alias of `/now_` (grace-held rows). |
| `/forcestart_<n\|hash>` | Arm the lane-cap bypass for a queued/moving row or group (in-memory, disarms on lane exit or restart). |
| `/skip_<hash>` | Hold a release: no workers, no Prowlarr queries, no downloads, no moves until it is resumed. The row and its state are kept. |
| `/unskip_<hash>` | Legacy alias of `/resume_`. |
| `/resume_<hash>` | Resume a held row, and lift a cancelled (ignored) hash so it is tracked again. A full 40-char hash also reaches ignored entries, which are invisible in the list. |
| `/unignore_<hash>` | Legacy alias of `/resume_` (full 40-char hash only). |
| `/unignore <hash>` | Manual form of `/unignore_` (full 40-char hash only). |
| `/injectfuse_<n\|hash>` | Manual fuse seeding for content you moved to the remote yourself (group number or tracked hash/prefix). |
| `/injectfuse <hash>` | Manual fuse seeding for any VPS1 infohash, tracked or not yet tracked. |
| `/act_<n\|hash>` | Action sheet for a group: only the currently eligible actions are offered, as hash-direct buttons. |

## Buttons

### Active Tasks card

- **Pagination** — `◀ / ▶` move between pages; the page number follows
  the filtered view.
- **Status filters** — `All`, `DL` (downloading), `MV` (moving),
  `RD` (re-adding), `QD` (queued + waiting), plus the aggregate
  overview. The filter narrows the render *and* the numbering; the
  destructive scope always matches what the list showed.
- **Per-row buttons** — act on one copy: cancel, start now, skip/resume.

### Cancel / forget sheets (two questions)

1. **Remember?** — `🚫 Ignore + forget` (the release is blocked from
   coming back) or `👻 Just forget` (it can be re-added later). `Close`
   aborts.
2. **Files?** — keep the local files or delete them (deleting is skipped
   for anything living on the fuse mount: remote bytes are never removed
   through the mount).

Bulk scopes (`/cancel_match`, `/cancel_stalled`, `/cancel_status`) ask the
same two questions once for the whole scope.

### Inject sheet

- `✔ Yes, inject` / `✖ No` — confirm that you moved the bytes to the
  remote yourself, or abort with `Close`.

### Start-now sheet

- `▶ Start <copy> now` — pick which copy of the group to start.

## Notes

- `abort` / `Close` cancels any open sheet without changing anything.
- Buttons are bound to the operator who opened the sheet; another
  operator tapping them gets "this sheet belongs to another operator".
- A second tap on a destructive scope while one is still running answers
  "Already working on it" instead of executing twice.
- `/find`, `/files_` and `/stats` are read-only: they never queue or
  download anything.
