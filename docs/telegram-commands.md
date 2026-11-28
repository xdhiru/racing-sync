# Telegram command reference

Everything the operator can type or tap in the Telegram chat. The bot is
single-operator: sheets are bound to whoever opened them, and destructive
buttons are only answered for that operator.

## Two ways to type a target

Every command that needs a target accepts **both** separators:

```
/cancel 3          ← typed by hand (a space is easier to type)
/cancel_3          ← the older shape, still accepted
```

The target is a **group number** as shown in the Active Tasks card (`3`), or
a tracked infohash / hash prefix. The slash popup lists the bare verb
(`/cancel`), so you can always discover the command by typing `/` instead of
remembering it. Typing a bare verb that needs a target (`/start`) answers
with its usage line.

All commands accept an optional `@bot` suffix in group chats
(`/active@my_bot`).

## The slash popup (every command the bot answers)

| Command | What it does |
|---|---|
| `/active` | Send the Active Tasks card now (recovery when the card was deleted). |
| `/add` | Ingest the `.torrent` you replied to (or sent with `/add` as the caption) as a Telegram-origin drop. |
| `/act <target>` | Action sheet for a group: only the currently eligible actions, as hash-direct buttons. |
| `/cancel <target>` | Cancel one release (asks the remember + keep/delete questions). No argument: cancel by lane. |
| `/cancel_all <text>` | Cancel every live group whose title contains `<text>`. |
| `/cancel_lane` | Cancel a whole lane (DL/MV/RD/QD) — a sheet with live counts. |
| `/cancel_stalled` | Cancel every download with no seeders and no progress. |
| `/files <target>` | Read-only file listing for one torrent/group. |
| `/find <text>` | Read-only Prowlarr search over the download-target indexers. |
| `/force <target>` | Start a queued/moving row past the lane caps (in-memory; disarms on lane exit or restart). |
| `/hold <target>` | Hold a release: no workers, no Prowlarr queries, no downloads, no moves until it is unheld. |
| `/inject <hash>` | Manual fuse seeding — you moved the bytes to the remote yourself. |
| `/pause` | Pause intake — no new downloads start until `/resume`. |
| `/resume` | Resume intake of new downloads. |
| `/start <target>` | Start now instead of waiting: a waiting row uses its starting torrent, a grace-held row starts at once. |
| `/stats` | Pipeline snapshot: rows by state, workers, intake, ignored count, storage footer. |
| `/tick` | Run one full pipeline tick now (poll + schedule + janitor). |
| `/unhold <hash>` | Resume a held release. A full 40-char hash also lifts a cancelled (ignored) release. |
| `/unignore <hash>` | Track a cancelled release again. |

## Older spellings (still work, not advertised)

| Old | New |
|---|---|
| `/now_<target>`, `/fetch_<target>`, `/prefer_<target>` | `/start <target>` |
| `/forcestart_<target>` | `/force <target>` |
| `/skip_<target>` | `/hold <target>` |
| `/unskip_<target>`, `/resume_<target>` | `/unhold <target>` |
| `/unignore_<hash>` | `/unignore <hash>` |
| `/injectfuse_<target>`, `/injectfuse <hash>` | `/inject <target>` |
| `/cancel_match <text>` | `/cancel_all <text>` |
| `/cancel_status` | `/cancel_lane` |
| `/pause_intake`, `/resume_intake` | `/pause`, `/resume` |

## Buttons

### Active Tasks card

- **Pagination** — `◀ / ▶` move between pages; the page number follows
  the filtered view.
- **Status filters** — `All`, `DL` (downloading), `MV` (moving),
  `RD` (re-adding), `QD` (queued + waiting), plus the aggregate overview.
  The filter narrows the render *and* the numbering; the destructive
  scope always matches what the list showed.
- **Per-group command** — each group shows `/act <n>`, the single entry
  point with the eligible actions.
- **Per-row buttons** — act on one copy: cancel, start now, hold/unhold.

### Cancel / forget sheets (two questions)

1. **Remember?** — `🚫 Ignore + forget` (the release is blocked from
   coming back) or `👻 Just forget` (it can be re-added later). `Close`
   aborts.
2. **Files?** — keep the local files or delete them (deleting is skipped
   for anything living on the fuse mount: remote bytes are never removed
   through the mount).

Bulk scopes (`/cancel_all`, `/cancel_stalled`, `/cancel_lane`) ask the same
two questions once for the whole scope.

### Inject sheet

- `✔ Yes, inject` / `✖ No` — confirm that you moved the bytes to the
  remote yourself, or abort with `Close`.

### Start sheet

- `▶ Start <copy> now` — pick which copy of the group to start.

## Notes

- `abort` / `Close` cancels any open sheet without changing anything.
- Buttons are bound to the operator who opened the sheet; another operator
  tapping them gets "this sheet belongs to another operator".
- A second tap on a destructive scope while one is still running answers
  "Already working on it" instead of executing twice.
- `/find`, `/files` and `/stats` are read-only: they never queue or
  download anything.
