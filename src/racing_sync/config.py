"""Typed configuration schema.

Loads from TOML and validates cross-field constraints (e.g. Deluge source
requires SFTP, rclone remote paths must be `name:path/`).
"""

from __future__ import annotations

import functools
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class NginxAuthConfig(BaseModel):
    """Authentication for an HTTP layer (typically nginx) sitting in front
    of the torrent WebUI.

    Two modes are supported, controlled by `mode`:

    1. `mode = "basic"` (default for nginx `auth_basic`):
       The proxy responds with `401 Unauthorized` + `WWW-Authenticate:
       Basic realm=...` and the client sends an `Authorization: Basic`
       header on every request. **No separate URL needed** — the
       credentials are taken from the parent `[source]` / `[dest]`
       `username` and `password` fields.

    2. `mode = "form_post"`:
       The proxy presents an HTML login form; the client POSTs
       `extra_fields + {user_field: <username>, pass_field: <password>}`
       to `url` to obtain a session cookie, then proceeds with the
       WebUI's own login. Use this for custom `auth_request` flows
       where the proxy returns a login HTML page on 401 instead of a
       Basic challenge.

    In practice, **most nginx setups use mode 1** and you can leave
    `url = ""` entirely. Mode 2 is for unusual setups.
    """

    mode: Literal["basic", "form_post", "off"] = "off"
    # Mode 2 only: the POST endpoint.
    url: str = ""
    # Mode 2 only: HTML form field names.
    user_field: str = "username"
    pass_field: str = "password"
    extra_fields: dict[str, str] = {}

    @model_validator(mode="after")
    def _check_url(self) -> NginxAuthConfig:
        if self.mode == "form_post" and not self.url.strip():
            raise ValueError('nginx mode="form_post" requires a non-empty url')
        return self


class HTTPClientConfig(BaseModel):
    """Internal helper used by clients.http_base.

    Built from a SourceConfig/DestConfig via from_xxx() helpers below.
    """

    host: str
    username: str = ""
    password: SecretStr = SecretStr("")
    nginx_mode: Literal["basic", "form_post", "off"] = "off"
    nginx_url: str = ""
    nginx_user_field: str = "username"
    nginx_pass_field: str = "password"
    nginx_extra_fields: dict[str, str] = {}
    # False (default) forces IPv4 for the WebUI session — intentional, see
    # [source].use_ipv6. Plumb-through only; set it on [source]/[dest].
    use_ipv6: bool = False

    def has_nginx(self) -> bool:
        return self.nginx_mode in ("basic", "form_post")

    @classmethod
    def from_source(cls, src: "SourceConfig") -> "HTTPClientConfig":
        return cls(
            host=src.host,
            username=src.username,
            password=src.password,
            nginx_mode=src.nginx.mode,
            nginx_url=src.nginx.url,
            nginx_user_field=src.nginx.user_field,
            nginx_pass_field=src.nginx.pass_field,
            nginx_extra_fields=dict(src.nginx.extra_fields),
            use_ipv6=bool(getattr(src, "use_ipv6", False)),
        )

    @classmethod
    def from_dest(cls, dst: "DestConfig") -> "HTTPClientConfig":
        return cls(
            host=dst.host,
            username=dst.username,
            password=dst.password,
            nginx_mode=dst.nginx.mode,
            nginx_url=dst.nginx.url,
            nginx_user_field=dst.nginx.user_field,
            nginx_pass_field=dst.nginx.pass_field,
            nginx_extra_fields=dict(dst.nginx.extra_fields),
            use_ipv6=bool(getattr(dst, "use_ipv6", False)),
        )


class DelugeSFTPConfig(BaseModel):
    """SSH/SFTP credentials for fetching .torrent files from a Deluge state dir.

    Required when [source].type = "deluge" because Deluge's WebUI doesn't
    expose a clean way to download the .torrent bytes for an infohash.

    Authentication priority (first match wins):
      1. SSH key file  (`ssh_key_path`) + optional passphrase
         (`ssh_key_passphrase`)
      2. SSH password  (`ssh_password`)

    A non-empty `ssh_key_path` takes precedence over `ssh_password` even if
    both are set. If neither is set, config validation fails.
    """

    enabled: bool = False
    ssh_host: str = "127.0.0.1"
    ssh_port: int = Field(default=22, ge=1, le=65535)
    ssh_user: str = "deluge"
    # Plain password auth (used only when no key file is provided).
    ssh_password: SecretStr = SecretStr("")
    # Public-key auth
    ssh_key_path: Path | None = None
    ssh_key_passphrase: SecretStr = SecretStr("")
    known_hosts_path: Path | None = None
    auto_add_host_key: bool = False
    state_dir: Path
    # Independent SSH/SFTP connections in the exporter pool. Re-inject
    # bursts from concurrent workers used to serialize on one transport
    # (15s-timeout clusters); 3 lets a few fetches fly in parallel while
    # a wedged member fails over instead of wedging everyone.
    pool_size: int = Field(default=3, ge=1, le=8)

    @field_validator("ssh_key_path", "known_hosts_path", mode="before")
    @classmethod
    def _empty_path_is_none(cls, v: object) -> object:
        """TOML "" coerces to Path(".") which is truthy; treat as None."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @model_validator(mode="after")
    def _check_auth(self) -> "DelugeSFTPConfig":
        # Only enforce credentials when the section is actually in use.
        has_pwd = bool(
            self.ssh_password.get_secret_value().strip()
            if hasattr(self.ssh_password, "get_secret_value")
            else str(self.ssh_password).strip()
        )
        if self.enabled and not self.ssh_key_path and not has_pwd:
            raise ValueError(
                "Deluge SFTP requires ssh_password or ssh_key_path when enabled"
            )
        return self


class SourceConfig(BaseModel):
    type: Literal["qbittorrent", "deluge"]
    host: str
    username: str = ""
    password: SecretStr = SecretStr("")
    # Filter torrents on the racing client by their category / label.
    # Empty string (default) means "match all categories" — every
    # torrent on the racing client is eligible for sync. This is the
    # recommended setting when the racing client does NOT organise its
    # torrents into a dedicated category like "racing".
    #
    # Examples:
    #   category = ""           # sync every torrent
    #   category = "racing"     # only sync torrents in category "racing"
    #   category = "auto,manual"  # qBittorrent supports comma-separated names
    #
    # Deluge uses the term "label" but the field is unified under
    # `category` for simplicity; the coordinator calls the appropriate
    # filter parameter on each client.
    category: str = ""
    # Minimum age of a torrent (seconds since added on the racing
    # client) before it is eligible for sync. Gives cross-seeds and
    # other private tracker torrents time to be added to the racing
    # client before sync and re-injection begin.
    #
    # 0 (default) — sync every torrent immediately, regardless of age.
    #
    # 3600 (1 hour) — only sync torrents that are at least 1 hour old.
    #
    # 86400 (24 h) — only sync torrents that are at least 24 hours old.
    min_age_seconds: int = Field(default=0, ge=0)
    # Optional nginx basic-auth in front of the WebUI. Leave url empty if
    # nginx is not used.
    nginx: NginxAuthConfig = NginxAuthConfig()
    deluge_sftp: DelugeSFTPConfig | None = None
    # Network family for the WebUI session. False (default) forces IPv4:
    # intentional — tracker allowlists, split-horizon DNS and jail/VPN
    # egress on seedboxes are overwhelmingly v4, and dual-stack happy-eyeballs
    # to link-local/ULA addresses has wedged handshakes. Set true for v6.
    use_ipv6: bool = False

    @model_validator(mode="after")
    def _deluge_needs_sftp(self) -> "SourceConfig":
        if self.type == "deluge" and (
            self.deluge_sftp is None or not self.deluge_sftp.enabled
        ):
            raise ValueError("Deluge source requires [source.deluge_sftp] enabled")
        return self


class DestConfig(BaseModel):
    host: str
    username: str = ""
    password: SecretStr = SecretStr("")
    save_path: Path
    # Optional nginx basic-auth in front of the qBittorrent WebUI on VPS2.
    nginx: NginxAuthConfig = NginxAuthConfig()
    # Maximum concurrent torrents actively downloading on VPS2 SSD (default: 3)
    max_active_downloads: int = Field(default=3, ge=1, le=100)
    # Network family for the WebUI session. False (default) forces IPv4:
    # intentional — see [source].use_ipv6.
    use_ipv6: bool = False


class SSDConfig(BaseModel):
    path: Path
    max_inflight_bytes: int = Field(ge=1)
    skip_movie_larger_than_bytes: int = Field(ge=1)
    safety_margin_bytes: int = Field(default=0, ge=0)


class RemoteConfig(BaseModel):
    default: str
    unsorted: str

    @field_validator("default", "unsorted")
    @classmethod
    def _must_look_remote(cls, v: str) -> str:
        v = v.strip()
        m = re.match(r"^([A-Za-z0-9_\-]+):(.*)$", v)
        if not m:
            raise ValueError(
                f"rclone remote path must be of form 'name:path', got: {v!r}"
            )
        name, path = m.group(1), m.group(2)
        # Reject Windows drive letters (C:/, C:\) mistaken for remotes.
        if len(name) == 1 and path.startswith(("/", "\\")):
            raise ValueError(
                f"rclone remote path looks like a Windows drive, got: {v!r}"
            )
        if not path or path.strip() in ("", "/"):
            raise ValueError(
                f"rclone remote path must include a path after 'name:', got: {v!r}"
            )
        if not v.endswith("/"):
            raise ValueError(f"rclone remote path must end with '/', got: {v!r}")
        return v


class FuseConfig(BaseModel):
    mount: Path
    mount_unsorted: Path
    # Delay in seconds before re-injecting torrents to fuse after rclone move (default: 30)
    reinject_delay_seconds: int = Field(default=30, ge=0)
    # Gap in seconds between immediate re-injection retries when WebUI is unresponsive (default: 120 / 2 min)
    reinject_retry_gap_seconds: int = Field(default=120, ge=1)
    # Backoff interval in seconds between retry cycles when WebUI remains unresponsive (default: 1800 / 30 min)
    reinject_backoff_seconds: int = Field(default=1800, ge=1)
    # Maximum elapsed seconds since re-injection began before marking FAILED (default: 86400 / 24 hours)
    reinject_max_age_seconds: int = Field(default=86400, ge=60)


class RcloneConfig(BaseModel):
    binary: Path = Path("/usr/local/bin/rclone")
    config_path: Path | None = None
    remote: RemoteConfig
    fuse: FuseConfig
    extra_move_flags: list[str] = Field(default_factory=list)
    batch_move_extra_flags: list[str] = Field(default_factory=list)
    # Maximum concurrent rclone move commands running simultaneously (default: 3)
    max_concurrent_moves: int = Field(default=3, ge=1, le=100)
    reinject_delay_seconds: int | None = Field(default=None, ge=0)

    @field_validator("config_path", mode="before")
    @classmethod
    def _empty_is_none(cls, v: object) -> object:
        """Treat empty string in TOML as None so rclone uses user default config."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("extra_move_flags", "batch_move_extra_flags")
    @classmethod
    def _reject_config_hijack_flags(cls, v: list[str]) -> list[str]:
        """Reject rclone flags that hijack config/credentials.

        ``--config`` (any form) swaps the whole rclone config; anything
        matching ``--password-command`` / ``--ask-password`` executes or
        prompts. Both are footguns in a tuning list — everything else
        (transfers, bwlimit, s3-chunk-size, ...) passes through.
        """
        bad: list[str] = []
        for item in v or []:
            try:
                low = str(item).strip().lower()
            except Exception:
                continue
            flag = low.split("=", 1)[0]
            if flag in ("--config", "--password-command", "--ask-password"):
                bad.append(str(item))
        if bad:
            raise ValueError(
                "rclone move flags must not include config/credential hijack "
                f"flags (--config, --password-command, --ask-password), got: {bad!r}"
            )
        return v


class ClassifierConfig(BaseModel):
    episode_regex: str = r"(?i)\bS\d{1,2}E\d{1,3}\b"

    @field_validator("episode_regex")
    @classmethod
    def _valid_regex(cls, v: str) -> str:
        re.compile(v)  # raises if invalid
        return v

    @property
    def _episode_re(self) -> re.Pattern[str]:
        cached = self.__dict__.get("_cached_re_tuple")
        if cached and cached[0] == self.episode_regex:
            return cached[1]
        compiled = re.compile(self.episode_regex)
        self.__dict__["_cached_re_tuple"] = (self.episode_regex, compiled)
        return compiled


class TelegramConfig(BaseModel):
    """Telegram layout:

      - one message per torrent (detail card, edited in place as state
        advances, persists in chat as history)
      - one pinned message at the bottom of the chat (active tasks list,
        edited every `status_update_interval` seconds)

    App logging goes to local files only — there is no log forwarding
    to Telegram.
    """
    enabled: bool = False
    bot_token: SecretStr = SecretStr("")
    chat_id: str = ""
    status_update_interval: int = Field(default=45, ge=5)
    pin_status_message: bool = False
    # Outbound rate limit for per-torrent detail messages (and
    # anything else we send). Telegram's bot API allows roughly 30
    # requests/sec across all chats. We self-throttle to `outbound_rate`
    # per second so a burst of state transitions (e.g. first run on a
    # racing client with 60+ torrents) doesn't trigger HTTP 429.
    outbound_rate: int = Field(default=5, ge=1)
    # Number of active tasks displayed per page in the status message (default: 5)
    page_size: int = Field(default=5, ge=1)
    # Re-post (delete + resend, silent) the active-tasks message when our
    # own newer traffic has buried it, at most every N seconds, so it
    # returns to newest-message position. Never fires while already last.
    # 0 = disabled (edit in place, today's behavior). Values 1-4 are
    # clamped to 5 to respect Telegram rate limits.
    active_repost_interval_seconds: int = Field(default=0, ge=0)
    @model_validator(mode="after")
    def _validate(self) -> "TelegramConfig":
        if self.enabled:
            token = self.bot_token.get_secret_value().strip()
            if not token or token.upper() in ("CHANGE_ME", "YOUR_BOT_TOKEN"):
                raise ValueError("telegram.bot_token is required and cannot be a placeholder when enabled")
            chat = self.chat_id.strip()
            if not chat or chat.upper() in ("CHANGE_ME", "YOUR_CHAT_ID"):
                raise ValueError("telegram.chat_id is required and cannot be a placeholder when enabled")
        return self


class LoggingSinkConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    auth_token: SecretStr = SecretStr("")
    forward_min_level: Literal[
        "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    ] = "INFO"

    @model_validator(mode="after")
    def _validate(self) -> "LoggingSinkConfig":
        if self.enabled:
            if not self.url or not self.url.strip().startswith(("http://", "https://")):
                raise ValueError("logging_sink.url must be a valid http(s) URL when enabled")
        return self


class WatchDirConfig(BaseModel):
    """req #3: manual private torrents dropped here are processed."""

    path: Path
    # Glob of filenames to accept (lowercase). Anything else is ignored.
    glob: str = "*.torrent"
    # If true, delete the .torrent after it has been picked up.
    delete_after_pickup: bool = True
    # Prowlarr query policy for watch-dir drops
    query_prowlarr: bool = True
    # If prowlarr returns a hit on the configured indexer, use that torrent
    # for SSD download. Otherwise fall back to the dropped file itself.
    prefer_prowlarr_result: bool = True


class ProwlarrTrackerMap(BaseModel):
    """req #4: announce URL substring → prowlarr indexer name.

    The TOML shape is a flat dict under `[prowlarr.tracker_map]`, e.g.:

        [prowlarr.tracker_map]
        "aither.cc"      = "Aither (API)"
        "beyond-hd"      = "BeyondHD"
        "animebytes.tv"  = "AnimeBytes"

    The first substring (case-insensitive) that appears in the racing
    client's announce URL wins, returning the corresponding indexer
    name. Put more specific substrings before more general ones.

    Fully user-defined. The app does not assume any particular private
    trackers. If you don't list a substring, racing-client torrents
    using that announce URL won't be cross-seeded.
    """

    model_config = {"extra": "allow"}

    entries: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: object) -> object:
        """Accept either:
          - a flat dict:        {"aither.cc": "Aither (API)"}
          - the existing shape: {"entries": {...}}
        """
        if isinstance(data, dict):
            # Detect the OLD schema (which had `_substr` / `_index`
            # fields or a nested `overrides` table) and fail loudly so
            # users get a clear migration error instead of silently
            # broken cross-seed matching.
            for forbidden in ("overrides", "beyond_hd_substr",
                              "beyond_hd_index", "aither_substr",
                              "aither_index", "animebytes_substr",
                              "animebytes_index"):
                if forbidden in data:
                    raise ValueError(
                        f"[prowlarr.tracker_map] uses the old schema. "
                        f"`{forbidden}` is no longer supported. "
                        f"Use a flat dict instead, e.g.:\n"
                        f"  [prowlarr.tracker_map]\n"
                        f'  "aither.cc" = "Aither (API)"\n'
                        f'  "beyond-hd" = "BeyondHD"'
                    )
            if "entries" in data and isinstance(data["entries"], dict):
                entries = data["entries"]
            else:
                # Otherwise the input is the flat dict.
                entries = {str(k): str(v) for k, v in data.items()}
            for k, val in entries.items():
                if not str(k).strip():
                    raise ValueError(
                        "[prowlarr.tracker_map] has an empty substring key "
                        "which would match every announce URL"
                    )
                if not str(val).strip():
                    raise ValueError(
                        f"[prowlarr.tracker_map] entry {k!r} has an empty indexer name"
                    )
            return {"entries": entries}
        return data

    def resolve(self, announce_url: str) -> str | None:
        if not announce_url:
            return None
        low = announce_url.lower()
        for sub, name in self.entries.items():
            if not sub or not sub.strip():
                continue
            if sub.lower() in low:
                return name
        return None


class ProwlarrConfig(BaseModel):
    """req #5 + #6: Prowlarr integration."""

    enabled: bool = False
    base_url: str = ""       # e.g. http://127.0.0.1:9696
    api_key: SecretStr = SecretStr("")
    # req #6: the indexer used for SSD downloads + cross-seed searches.
    # No default — you MUST set this when [prowlarr].enabled = true,
    # because the name must match exactly what your Prowlarr instance
    # calls the indexer (it's case-sensitive).
    download_indexer: str = ""
    # Timeout for HTTP calls to prowlarr
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    # How many results to consider from a search
    max_results: int = Field(default=20, ge=1, le=1000)
    # Tracker substring → indexer name map
    tracker_map: ProwlarrTrackerMap = ProwlarrTrackerMap()
    # Announce URL substrings that identify torrents belonging to the download indexer
    download_indexer_substrings: list[str] = Field(default_factory=list)
    # Substrings in torrent titles to skip querying Prowlarr for (case-insensitive)
    skip_query_substrings: list[str] = Field(default_factory=list)
    # Network family for the Prowlarr session. False (default) forces IPv4:
    # intentional — see [source].use_ipv6.
    use_ipv6: bool = False

    def is_download_indexer(self, announce_url: str) -> bool:
        """Check if an announce URL matches the download indexer."""
        if not announce_url:
            return False
        low = announce_url.lower()
        for sub in self.download_indexer_substrings:
            if sub and sub.lower() in low:
                return True
        return False

    def should_skip_title(self, title: str) -> bool:
        """True iff title contains any of skip_query_substrings (case-insensitive)."""
        if not title or not self.skip_query_substrings:
            return False
        low = title.lower()
        return any(sub and sub.lower() in low for sub in self.skip_query_substrings)

    @model_validator(mode="after")
    def _validate(self) -> "ProwlarrConfig":
        if self.enabled:
            if not self.base_url.startswith(("http://", "https://")):
                raise ValueError("prowlarr.base_url must start with http(s)://")
            key = self.api_key.get_secret_value().strip() if isinstance(self.api_key, SecretStr) else str(self.api_key).strip()
            if not key or key.upper() in ("CHANGE_ME", "YOUR_PROWLARR_API_KEY"):
                raise ValueError("prowlarr.api_key required and cannot be a placeholder when enabled")
            if not self.download_indexer:
                raise ValueError(
                    "prowlarr.download_indexer required when enabled. "
                    "Set it to the exact name of the indexer in your "
                    "Prowlarr instance (case-sensitive)."
                )
            if not self.download_indexer_substrings:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "prowlarr enabled but download_indexer_substrings is empty — "
                    "is_download_indexer() will always return False and "
                    "sacrificial detection is disabled",
                )
        return self


class CrossSeedConfig(BaseModel):
    """req #1 + #2: how to pick the SSD source torrent and what to inject."""

    # When the racing client has multiple torrents for the same content,
    # we want to download the public one on VPS2 SSD. If true, that public
    # torrent is re-fetched via prowlarr (qBittorrent's own cross-seed would
    # create duplicates). If false, the original public torrent from VPS1 is
    # exported via SFTP if available, else queried via prowlarr.
    refetch_public_via_prowlarr: bool = False
    # req #2 follow-up: if the configured download_indexer returns no hit
    # (release too new), retry the query every
    # `prowlarr_retry_interval_seconds`. Give up after
    # `prowlarr_max_age_seconds` since the FIRST query attempt and mark
    # the torrent FAILED for manual handling.
    #
    # The names use "prowlarr_" because the search runs through Prowlarr,
    # but the *indexer* being queried is whatever the user configured
    # under [prowlarr].download_indexer (default "Seedpool (API)").
    prowlarr_retry_interval_seconds: int = Field(default=1800, ge=60)  # 30 min
    prowlarr_max_age_seconds: int = Field(default=86400, ge=3600)       # 24 h

    @property
    def seedpool_retry_interval_seconds(self) -> int:
        return self.prowlarr_retry_interval_seconds

    @property
    def seedpool_max_age_seconds(self) -> int:
        return self.prowlarr_max_age_seconds

    # Strategy flags --------------------------------------------------
    #
    # `inject_racing_torrents_to_fuse` (default true):
    #   When true, after the SSD download + rclone move complete, every
    #   torrent that the racing client already has for this content is
    #   re-added on the VPS2 qBittorrent pointing at the fuse mount, with
    #   skip_check=true. This is the PRIMARY way the racing torrents
    #   reach VPS2.
    #
    #   When false, only the cross-seed torrent (Prowlarr-fetched or
    #   racing-client-exported .torrent) is re-added. The original racing
    #   torrents remain on VPS1 only.
    #
    # `allow_prowlarr_cross_seed` (default true):
    #   When true, and the racing client has no public torrent for the
    #   content (or the user prefers a Seedpool cross-seed), we query
    #   Prowlarr on the configured download_indexer to obtain a .torrent
    #   for SSD download. Disable this if you want VPS2 to always leech
    #   from the racing client's own torrents (e.g. via SFTP export from
    #   Deluge / qBittorrent state).
    #
    # `allow_ssh_export` (default true):
    #   When true, we may obtain a .torrent from VPS1 via SFTP / SSH
    #   (Deluge state dir or qB BT_backup). Used as the LAST resort when
    #   neither a public racing torrent nor a Prowlarr cross-seed is
    #   available.
    #
    # Note: for Deluge sources, SFTP is the only way to obtain the .torrent
    # bytes for the racing client's own torrents. The coordinator will
    # automatically enable this and refuse to start if SFTP credentials
    # are missing.
    inject_racing_torrents_to_fuse: bool = True
    allow_prowlarr_cross_seed: bool = True
    allow_ssh_export: bool = True

    @model_validator(mode="after")
    def _check_strategy(self) -> "CrossSeedConfig":
        if not self.allow_prowlarr_cross_seed and not self.allow_ssh_export:
            raise ValueError(
                "cross_seed: allow_prowlarr_cross_seed=false + allow_ssh_export=false "
                "leaves no SSD-source strategy for private torrents "
                "(every torrent would park in WAITING_SEEDPOOL until FAILED). "
                "Enable at least one unless this is a public-only deployment."
            )
        return self


class RecoveryConfig(BaseModel):
    """req #4: on startup, reconcile qBittorrent on VPS2 with state DB
    and filesystem (SSD + fuse)."""

    # Run reconciliation on startup
    run_on_startup: bool = True
    # Allowed transition states when reconciling (anything else = force fix)
    auto_fix_state: bool = True
    # How many torrents to inspect per pass (avoid hammering API)
    batch_size: int = Field(default=50, ge=1, le=1000)
    # Reset previously-FAILED rows back to NEW on startup so they
    # get re-processed with the current code. Useful after a fix that
    # would have prevented the failure in the first place (e.g. a
    # new Deluge RPC fallback, a credentials change, etc.).
    # Set false if you want FAILED rows to stay failed for manual
    # intervention.
    auto_retry_failed: bool = True
    # Maximum number of times a FAILED row will be auto-retried on boot
    # before being left in FAILED to avoid infinite failure loops.
    max_failed_retries: int = Field(default=3, ge=0, le=100)



class CleanupConfig(BaseModel):
    """VPS1 racing-client cleanup: reclaim disk after VPS2 owns the content.

    Disabled by default (destructive — opt in deliberately). When enabled, an
    hourly janitor deletes racing-client torrents (+data) whose content VPS2
    has fully secured, oldest-eligible-first, capped per run:

    - Normal path: row DONE + fuse entries re-verified + adaptive grace
      elapsed (or race verifiably idle past `idle_confirm_minutes`).
    - Early pressure path: row in MOVING/RE_ADDING (bytes 100% on VPS2 SSD,
      pipeline mid-flight) + VPS1 group idle + free space under the low
      watermark. VPS2 never pulls content bytes from VPS1 (SFTP carries only
      the .torrent file), so a secured SSD copy makes VPS1's redundant.

    Grace adapts to pressure so full autobrr days clear fast and quiet days
    keep seeding: `grace = max(min_grace, min(space_curve, velocity_curve))`
    where space_curve interpolates free space between the low/high
    watermarks and velocity_curve interpolates intake arrivals/hour between
    the calm/burst rates. Minimums (min_ratio/min_seed_hours) default to 0
    (disabled) — valid only when both clients seed the SAME tracker account,
    so VPS2's long-term fuse seeding keeps the account compliant.
    """

    enabled: bool = False
    # Log what would be deleted without deleting anything. Run with true
    # for a while and review before allowing real deletes.
    dry_run: bool = True
    # Grace bounds (hours) after VPS2 marks DONE.
    min_grace_hours: float = Field(default=2.0, ge=0)
    max_grace_hours: float = Field(default=72.0, ge=0)
    # Free-space watermarks on VPS1 (bytes). At/below low -> min grace and
    # biggest-first ordering; at/above high -> max grace, oldest-first.
    low_watermark_free_bytes: int = Field(default=16106127360, ge=0)   # 15 GiB
    high_watermark_free_bytes: int = Field(default=42949672960, ge=0)  # 40 GiB
    # Below this, log loudly; grace stays floored at min (never zero).
    critical_watermark_free_bytes: int = Field(default=8589934592, ge=0)  # 8 GiB
    # Intake-velocity bounds (new racing releases/hour). At/above burst ->
    # min grace; at/below calm -> no shortening.
    burst_arrivals_per_hour: float = Field(default=8.0, ge=0)
    calm_arrivals_per_hour: float = Field(default=2.0, ge=0)
    # A race counts as finished after this many minutes with ~zero upload
    # AND zero leechers. Fast-lanes deletion past the grace wait.
    idle_confirm_minutes: float = Field(default=45.0, ge=0)
    # Upload rate at/below this (B/s) counts as quiet for idle detection.
    activity_upspeed_bps: int = Field(default=65536, ge=0)
    # Optional H&R minimums (0 = disabled). Enable only with per-account
    # needs; VPS2's fuse seeding normally keeps the account compliant.
    min_ratio: float = Field(default=0.0, ge=0)
    min_seed_hours: float = Field(default=0.0, ge=0)
    # Case-insensitive substrings; matching release names are never deleted.
    # Blank entries are rejected ("" would match every release).
    protected_patterns: list[str] = Field(default_factory=list)
    # Remove data files as well as client entries (required to free disk).
    # False = remove entries only (frees no space; useful for testing).
    delete_files: bool = True
    # Max content groups deleted per janitor run.
    per_run_cap: int = Field(default=10, ge=1)
    # Seconds between janitor runs.
    janitor_interval_seconds: int = Field(default=3600, ge=300)

    @model_validator(mode="after")
    def _check_bounds(self) -> "CleanupConfig":
        if self.max_grace_hours < self.min_grace_hours:
            raise ValueError("cleanup: max_grace_hours must be >= min_grace_hours")
        if self.high_watermark_free_bytes <= self.low_watermark_free_bytes:
            raise ValueError(
                "cleanup: high_watermark_free_bytes must be > low_watermark_free_bytes"
            )
        for pat in self.protected_patterns or []:
            if not str(pat).strip():
                raise ValueError(
                    "cleanup: protected_patterns must not contain blank entries "
                    "(an empty pattern matches every release)"
                )
        if self.critical_watermark_free_bytes > self.low_watermark_free_bytes:
            raise ValueError(
                "cleanup: critical_watermark_free_bytes must be <= low_watermark_free_bytes"
            )
        if self.burst_arrivals_per_hour <= self.calm_arrivals_per_hour:
            raise ValueError(
                "cleanup: burst_arrivals_per_hour must be > calm_arrivals_per_hour"
            )
        return self


class APIConfig(BaseModel):
    """Configuration for the HTTP API daemon.

    When `trust_nginx_header` is True, `X-Authenticated-User` is accepted
    from reverse proxies listed in `trusted_proxies`.

    SECURITY WARNING:
    Nginx MUST be configured to overwrite (never forward) `X-Authenticated-User`:
        proxy_set_header X-Authenticated-User $remote_user;
    Otherwise, an untrusted client can spoof the header and bypass authentication.
    """
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65535)
    trust_nginx_header: bool = False
    trusted_proxies: list[str] = ["127.0.0.1", "::1", "localhost"]
    api_token: SecretStr = SecretStr("")

    @model_validator(mode="after")
    def _validate(self) -> "APIConfig":
        if self.enabled:
            token = (
                self.api_token.get_secret_value().strip()
                if hasattr(self.api_token, "get_secret_value")
                else str(self.api_token).strip()
            )
            if not token and not self.trust_nginx_header:
                raise ValueError(
                    "api.api_token is required when api is enabled and trust_nginx_header is False"
                )
            if token and token.upper() in ("CHANGE_ME", "YOUR_API_TOKEN"):
                raise ValueError("api.api_token cannot be a placeholder when enabled")
        return self


class GeneralConfig(BaseModel):
    source_poll_interval: int = Field(default=30, ge=5)
    dest_poll_interval: int = Field(default=15, ge=5)
    state_db: Path = Path("/var/lib/racing-sync/state.db")
    log_dir: Path = Path("/var/log/racing-sync")
    log_retention_days: int = Field(default=14, ge=1)
    disk_safety_margin_bytes: int = Field(default=0, ge=0)
    # Optional overrides if specified under [general]
    max_active_downloads: int | None = Field(default=None, ge=1, le=100)
    max_concurrent_moves: int | None = Field(default=None, ge=1, le=100)
    download_stall_timeout_seconds: int = Field(default=0, ge=0)


class AppConfig(BaseModel):
    general: GeneralConfig
    source: SourceConfig
    dest: DestConfig
    ssd: SSDConfig
    rclone: RcloneConfig
    classifier: ClassifierConfig = ClassifierConfig()
    telegram: TelegramConfig = TelegramConfig()
    logging_sink: LoggingSinkConfig = LoggingSinkConfig()
    api: APIConfig = APIConfig()
    watch_dir: WatchDirConfig | None = None
    prowlarr: ProwlarrConfig = ProwlarrConfig()
    cross_seed: CrossSeedConfig = CrossSeedConfig()
    recovery: RecoveryConfig = RecoveryConfig()
    cleanup: CleanupConfig = CleanupConfig()

    @classmethod
    def from_toml(cls, path: str | Path) -> "AppConfig":
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib  # type: ignore[import-not-found,no-redef]

        with open(path, "rb") as f:
            data = tomllib.load(f)
        _warn_unknown_keys(cls, data)
        return cls.model_validate(data)

    @property
    def max_active_downloads(self) -> int:
        if self.general.max_active_downloads is not None:
            return self.general.max_active_downloads
        return self.dest.max_active_downloads

    @property
    def max_concurrent_moves(self) -> int:
        if self.general.max_concurrent_moves is not None:
            return self.general.max_concurrent_moves
        return self.rclone.max_concurrent_moves

    @property
    def fuse_reinject_delay_seconds(self) -> int:
        if self.rclone.reinject_delay_seconds is not None:
            return self.rclone.reinject_delay_seconds
        return self.rclone.fuse.reinject_delay_seconds

    @property
    def fuse_reinject_retry_gap_seconds(self) -> int:
        return self.rclone.fuse.reinject_retry_gap_seconds

    @property
    def fuse_reinject_backoff_seconds(self) -> int:
        return self.rclone.fuse.reinject_backoff_seconds

    @property
    def fuse_reinject_max_age_seconds(self) -> int:
        return self.rclone.fuse.reinject_max_age_seconds

    def is_episode(self, name: str) -> bool:
        return bool(self.classifier._episode_re.search(name))


def _warn_unknown_keys(model: type[BaseModel], data: object, prefix: str = "") -> None:
    """Log likely-typo config keys that pydantic would silently ignore.

    Most models use the default ``extra="ignore"`` (strict ``forbid`` would
    refuse to start on any typo'd historical key after an upgrade), so surf
    ``[general].max_active_download``-style mistakes as warnings instead.
    Models with ``extra="allow"`` (tracker maps) are skipped.
    """
    import logging as _logging

    try:
        if not isinstance(data, dict):
            return
        fields = getattr(model, "model_fields", {}) or {}
        try:
            extra = (getattr(model, "model_config", None) or {}).get("extra")
        except Exception:
            extra = None
        if extra == "allow":
            return
        for key, val in data.items():
            if key not in fields:
                _logging.getLogger(__name__).warning(
                    "config: unknown key %r%s — ignored (possible typo)",
                    key, f" in [{prefix}]" if prefix else "",
                )
                continue
            try:
                sub = fields[key].annotation
            except Exception:
                continue
            _descend_unknown(sub, val, f"{prefix}.{key}" if prefix else str(key))
    except Exception:
        pass


def _descend_unknown(annotation: object, val: object, prefix: str) -> None:
    """Recurse _warn_unknown_keys into nested BaseModel fields (best-effort)."""
    try:
        import types as _types
        import typing as _typing

        if not isinstance(val, dict):
            return
        origin = _typing.get_origin(annotation)
        args = [a for a in (_typing.get_args(annotation) or ()) if isinstance(a, type)]
        if origin in (_typing.Union, getattr(_types, "UnionType", _typing.Union)):
            for a in args:
                if isinstance(a, type) and issubclass(a, BaseModel):
                    _warn_unknown_keys(a, val, prefix)
                    return
            return
        ann = annotation
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            _warn_unknown_keys(ann, val, prefix)
    except Exception:
        pass