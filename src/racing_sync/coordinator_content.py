"""Stateless content helpers extracted from coordinator.py.

Pure functions with no Coordinator dependency: release-name
normalization, cross-seed verification, adaptive cleanup grace,
Telegram notify filter and public-tracker detection. Kept in a
separate module so coordinator.py stays focused on orchestration;
all names are re-exported from coordinator for backwards compat.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from .state import State

log = logging.getLogger(__name__)


def normalize_content_name(name: str) -> str:
    """Normalize release/torrent names for deduplication and grouping.

    Strips trailing indexer tags (e.g. '[A1B2C3D4]', '[FL]'), trailing
    file extensions ('.torrent', '.mkv', etc.), and case-folds/strips.
    Loops until stable so 'Show [A] [B]' and 'Movie.mkv.torrent' fully collapse.
    """
    s = (name or "").strip()
    for _ in range(5):
        prev = s
        s = re.sub(r"\.torrent$", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"\s*\[[^\]]+\]\s*$", "", s).strip()
        for ext in (".mkv", ".mp4", ".avi", ".ts", ".m4v"):
            if s.lower().endswith(ext):
                s = s[:-len(ext)].strip()
                break
        if s == prev:
            break
    return s.lower()


def size_within_tolerance(hit_size: int, target_size: int) -> bool:
    """True iff two byte sizes agree within min(50 MiB, 2%).

    A zero on either side means "unknown" and passes — callers only gate
    when both sides are known. Shared by the coordinator and Prowlarr
    matchers so the rule can't drift between selection and verification.
    """
    if hit_size > 0 and target_size > 0:
        tolerance = min(1024 * 1024 * 50, int(target_size * 0.02))
        return abs(hit_size - target_size) <= tolerance
    return True


def _matches_release(hit_title: str, hit_size: int, target_name: str, target_size: int) -> bool:
    """Check if a Prowlarr hit matches the target release by title and size."""
    ht_norm = normalize_content_name(hit_title)
    tg_norm = normalize_content_name(target_name)
    title_matches = (
        ht_norm == tg_norm
        or hit_title.strip().lower() == target_name.strip().lower()
    )
    if not title_matches:
        return False
    return size_within_tolerance(hit_size, target_size)


def _verified_cross_seed_blob(
    blob: bytes | None, *, target_name: str, target_size: int, hit_title: str
) -> tuple[bytes, str, str] | None:
    """Decode downloaded cross-seed bytes and prove they are the exact release.

    Index listings can drift from the payload behind them, so selection-time
    matching is re-checked against the authoritative decoded name/size here.
    Returns (blob, real_infohash, announce_url) or None — callers treat None
    exactly like "no hit" (park/retry/fallback), never downloading onward.
    """
    if not blob:
        return None
    try:
        from .watchdir import _bencoded_info_hash
        real_hash, blob_name, blob_size, announce = _bencoded_info_hash(blob)
    except Exception:
        log.warning("cross-seed download for %s is not a decodable torrent; ignoring",
                    target_name)
        return None
    try:
        from .prowlarr import release_title_matches
        ok = release_title_matches(blob_name or hit_title, blob_size or 0,
                                   target_name, target_size)
    except Exception:
        ok = False
    if not ok:
        log.warning("cross-seed mismatch for %s: got %r (%d B), want the exact "
                    "release; ignoring download",
                    target_name, blob_name, blob_size)
        return None
    return blob, (real_hash or "").lower(), announce or ""


def _lerp(x: float, x0: float, x1: float, y0: float, y1: float) -> float:
    """Linear interpolation of y over [x0, x1], clamped to [y0, y1]."""
    if x1 <= x0:
        return y1
    t = min(1.0, max(0.0, (x - x0) / (x1 - x0)))
    return y0 + t * (y1 - y0)


def cleanup_grace_seconds(cfg: object, free_bytes: int | None, arrivals_per_hour: float) -> float:
    """Adaptive VPS1-cleanup grace: the worse the pressure, the shorter the wait.

    `grace = max(min_grace, min(space_curve, velocity_curve))` where the
    space curve interpolates free bytes between the low/high watermarks and
    the velocity curve interpolates intake arrivals/hour between the
    calm/burst rates. Unknown free space degrades to the velocity curve
    alone (never to zero grace).
    """
    try:
        min_g = max(0.0, float(getattr(cfg, "min_grace_hours", 2.0) or 0.0)) * 3600.0
    except (TypeError, ValueError):
        min_g = 2.0 * 3600.0
    try:
        max_g = max(0.0, float(getattr(cfg, "max_grace_hours", 72.0) or 0.0)) * 3600.0
    except (TypeError, ValueError):
        max_g = 72.0 * 3600.0
    if max_g < min_g:
        max_g = min_g
    try:
        low = float(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
        high = float(getattr(cfg, "high_watermark_free_bytes", 0) or 0)
    except (TypeError, ValueError):
        low, high = 0.0, 0.0
    try:
        calm = float(getattr(cfg, "calm_arrivals_per_hour", 2.0) or 0.0)
        burst = float(getattr(cfg, "burst_arrivals_per_hour", 8.0) or 0.0)
    except (TypeError, ValueError):
        calm, burst = 2.0, 8.0
    try:
        rate = max(0.0, float(arrivals_per_hour or 0.0))
    except (TypeError, ValueError):
        rate = 0.0
    try:
        free_f = float(free_bytes) if free_bytes is not None else None
    except (TypeError, ValueError):
        free_f = None

    space_curve = _lerp(free_f, low, high, min_g, max_g) if free_f is not None else max_g
    velocity_curve = _lerp(rate, calm, burst, max_g, min_g)
    return max(min_g, min(space_curve, velocity_curve, max_g))


@dataclass(slots=True)
class SourceDecision:
    """Where the SSD-source torrent comes from."""

    torrent_bytes: bytes
    # "<indexer-slug>-cross-seed" for a download-target indexer hit
    # (e.g. "my-indexer-api-cross-seed"), "public-racing",
    # "public-<slug>-fallback", "private-sftp-fallback",
    # "private-export-fallback", "public-prowlarr", "watch-dir", ...
    source_label: str
    name: str
    size_bytes: int
    infohash: str
    announce_url: str = ""


def indexer_slug(name: str) -> str:
    """Slugify a Prowlarr indexer name for source labels.

    "My Indexer (API)" -> "my-indexer-api". Used to build per-indexer
    `source_label` values ("<slug>-cross-seed") so operators can see
    which download-target indexer supplied the SSD bytes.
    """
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "indexer"


# Telegram notification filter: only certain state transitions deserve
# a chat message. Passive discoveries (NEW on first tick) don't.
_TELEGRAM_NOTIFY_STATES = frozenset({
    State.QUEUED,
    State.DOWNLOADING,
    State.MOVING,
    State.RE_ADDING,
    State.DONE,
    State.FAILED,
    State.WAITING_INDEXER,
})


def _should_notify_telegram(prev: State, dst: State) -> bool:
    """Decide whether a state transition should fire a Telegram update.

    Policy:
      - First discovery of a torrent (NEW on its own) does NOT fire —
        prevents spam on first run with N pre-existing racing torrents.
      - Any transition INTO a work-active or terminal state fires
        (QUEUED, DOWNLOADING, MOVING, RE_ADDING, DONE, FAILED,
        WAITING_INDEXER).
      - The bot's per-torrent message will be created on the first
        such transition; subsequent updates just edit the same message.
    """
    if prev == dst:
        # Same-state re-entry shouldn't trigger duplicate notifications.
        return False
    return dst in _TELEGRAM_NOTIFY_STATES


PUBLIC_TRACKER_HOSTS = (
    "publicbt", "tracker.public", "tracker.openbittorrent",
    "opentrackr", "academictorrents", "bt.archlinux",
    "linuxmint", "ubuntu-releases", "nyaa", "animetosho",
    "tokyotosho", "torrentgalaxy", "1337x", "piratebay",
    "coppersurfer", "leechers-paradise", "open.stealth.si",
)


def _looks_public(tracker_urls: list[str]) -> bool:
    import urllib.parse as _up

    for url in tracker_urls:
        low = (url or "").strip().lower()
        if not low:
            continue
        try:
            host = (_up.urlsplit(low).hostname or "").lower()
        except Exception:
            host = ""
        hay = host or low
        for pub in PUBLIC_TRACKER_HOSTS:
            p = pub.lower()
            # Bounded substring on hostname: `nyaa` matches `nyaa.tracker.wf`
            # and `tracker.opentrackr.org`, but NOT `bannyaa.com` (where the
            # match is embedded in a longer alphanumeric label).
            try:
                if re.search(r"(?<![a-z0-9])" + re.escape(p) + r"(?![a-z0-9])", hay):
                    return True
            except re.error:
                if p in hay:
                    return True
    return False
