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

Commands the bot *prints* (Active Tasks card, detail cards) always use the
underscore form — `/act_3`, `/start_aaaaaaaaaa`, `/unhold_eeeeeeeeee` — so
the whole command is one word you can copy in one go and paste back into the
chat. The space form is for typing by hand.

All commands accept an optional `@bot` suffix in group chats
(`/active@my_bot`).

## Command list (exactly what the popup shows)

| Command | What it does |
|---|---|
| `/active` | Send the Active Tasks card now (recovery when the card was deleted). |
| `/add` | Ingest the `.torrent` you replied to (or sent with `/add` as the caption) as a Telegram-origin drop. |
| `/act <target>` | Action sheet for a group: only the currently eligible actions, as hash-direct buttons. |
| `/cancel <target>` | Cancel one release (asks the remember + keep/delete questions). No argument: cancel by lane. |
| `/cancel_all <text>` | Cancel every live group whose title contains `<text>`. |
| `/cancel_lane` | Cancel a whole lane (DL/MV/RD/QD) — a sheet with live counts. |
| `/cancel_stalled` | Cancel every download with no seeders and no progress. |
| `/clean <hash>` | Clean one completed (DONE) task's VPS1 copy now — skips the free-space trigger but keeps every safety check. |
| `/cleanup` | Run the VPS1 cleanup pass now — skips only the startup delay and the cadence; deletes nothing while VPS1 has room. |
| `/files <target>` | Read-only file listing for one torrent/group. |
| `/find <text>` | Read-only Prowlarr search over the download-target indexers. |
| `/force <target>` | Start a queued/moving row past the lane caps (in-memory; disarms on lane exit or restart). |
| `/hold <target>` | Hold a release: no workers, no Prowlarr queries, no downloads, no moves until it is unheld. |
| `/inject <hash>` | Manual fuse seeding — you moved the bytes to the remote yourself. |
| `/pause` | Pause intake — no new downloads start until `/resume`. |
| `/push_source <target>` | Copy that release from the racing box straight onto the seed remote VPS2 seeds from, instead of downloading a second copy. Needs `[source.rclone_push].enabled = true`. |
| `/resume` | Resume intake of new downloads. |
| `/start <target>` | Start now instead of waiting: a waiting row uses its starting torrent, a grace-held row starts at once. |
| `/stats` | Pipeline snapshot: rows by state, workers, intake, ignored count, storage footer. |
| `/tick` | Run one full pipeline tick now (poll + schedule + janitor). |
| `/unhold <hash>` | Resume a held release. A full 40-char hash also lifts a cancelled (ignored) release. |
| `/unignore <hash>` | Track a cancelled release again. |

There are no other spellings: the old names (`/now_`, `/skip_`,
`/injectfuse_`, `/cancel_match`, …) were removed, so typing one answers with
nothing (a bare known verb answers with its usage line instead).

## Buttons

### Active Tasks card

- **Pagination** — `◀ / ▶` move between pages; the page number follows
  the filtered view.
- **Status filters** — `All`, `DL` (downloading), `MV` (moving),
  `RD` (re-adding), `QD` (queued + waiting), plus the aggregate overview.
  The filter narrows the render *and* the numbering; the destructive
  scope always matches what the list showed.
- **`🗄` (still on VPS1)** — completed releases whose files the racing
  client still lists: `DONE` rows with at least one hash (source, dest,
  cross-seed or `injected_private_hashes`) present in the cached VPS1
  listing, oldest completion first, each line carrying the `🗄 still on
  VPS1` badge and the janitor's own headroom status (`secured · reclaiming`,
  `secured · waiting for space`, `not verifiable yet`).
  `DONE` shows in this tab only — `All` and every lane exclude it, and
  `cancel_lane` refuses it. The button disappears with
  `[cleanup].enabled = false` (a stored `🗄` filter then falls back to
  `All`), and while the VPS1 listing cannot be read the tab says
  `VPS1 listing unavailable` instead of inventing rows.
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

### Act sheet — VPS1 cleanup

- `🧹 Clean VPS1 copies` — shown only for a completed (DONE) group when
  `[cleanup].enabled` is on. Runs the same per-task cleanup as
  `/clean_<hash>`: fuse bytes verified, H&R minimums met, no race
  activity, un-injected `.torrent` bytes preserved; the free-space trigger is
  skipped on purpose (operator intent). With `dry_run = true` nothing is
  deleted and the reply says what would have been.
- The sheet also prints the row's headroom status (`Cleanup: secured ·
  waiting for space`, `Cleanup: not verifiable yet`). The same line follows a
  DONE row's stage text in the Active Tasks card: `secured` means the fuse
  copy verified and the row is kept until the space is needed, `reclaiming`
  means a pass is taking that space now, and `not verifiable yet` means the
  janitor will not delete it whatever the disk says.

### Act sheet — push from VPS1

- `📤 Push from VPS1…` — shown only when `[source.rclone_push].enabled =
  true` **and** a copy of the group is still in a state the push can take
  over (`NEW`, `WAITING_INDEXER`, `WAITING_DISK`, `QUEUED`), and only while
  no push for that group is already running. With the switch off the
  button is absent and the sheet looks exactly as it did before.
- Like Inject, it **asks first**: `✔ Yes, push from VPS1` starts the copy,
  `✖ No` and `Close` change nothing.
- One tap means **one push per group**: the command and the button both
  address the group, and the copy runs for its elected primary torrent —
  three tracker copies of the same release still transfer the bytes once.
- VPS1 runs one detached `rclone copy` of the files it already holds into
  the same remote VPS2 seeds from, so the bytes cross the internet once
  instead of twice. VPS2 never re-downloads them, and nothing is read or
  hashed on the mount or the SSD — names and sizes only.
- While the copy runs, the pipeline suppresses its own competing work for
  that group and parks the row; if the copy fails or passes its deadline,
  the row falls back to the normal path untouched. `/push_source` refuses
  (with the reason on one line) when VPS2 is already downloading, moving
  or seeding the content.
- A push is not only ever manual: with `auto_on_prowlarr_timeout` the
  expired indexer window prefers one over the VPS2 leech, and with
  `push_on_janitor_pressure` the cleanup janitor's own disk verdict starts
  one for releases already parked on "no cross-seed found" (no extra
  countdown involved — see `docs/architecture.md`).

## Notes

- `abort` / `Close` cancels any open sheet without changing anything.
- Buttons are bound to the operator who opened the sheet; another operator
  tapping them gets "this sheet belongs to another operator".
- A second tap on a destructive scope while one is still running answers
  "Already working on it" instead of executing twice.
- `/find`, `/files` and `/stats` are read-only: they never queue or
  download anything.
- **Cleanup** — the automatic janitor keeps every secured VPS1 copy seeding
  until the space is needed, then reclaims toward
  `[cleanup].high_watermark_free_bytes`. `/cleanup` and `/clean_<hash>` only
  bypass the waiting (cadence, and `/clean` the free-space trigger): fuse
  verification, H&R
  minimums, activity, and `.torrent` preservation always run — activity
  unless `[cleanup].ignore_activity_under_pressure` is on and this run
  reads real disk pressure, when VPS2 takes over serving the peer. `/clean`
  refuses non-DONE rows; MOVING copies are cleaned automatically under
  disk pressure. Both commands do nothing when `[cleanup].enabled` is
  false (or the chain adds no seed entry to verify against). A pass with
  nothing to delete answers `🧹 Cleanup: nothing deleted — nothing eligible
  (activity 17, verification 3)`, naming what held the rows back.
