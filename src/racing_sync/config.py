"""Typed configuration schema.

Loads from TOML and validates cross-field constraints (e.g. Deluge source
requires SFTP, rclone remote paths must be `name:path/`).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


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

    mode: Literal["basic", "form_post", "off"] = "basic"
    # Mode 2 only: the POST endpoint.
    url: str = ""
    # Mode 2 only: HTML form field names.
    user_field: str = "username"
    pass_field: str = "password"
    extra_fields: dict[str, str] = {}


class HTTPClientConfig(BaseModel):
    """Internal helper used by clients.http_base.

    Built from a SourceConfig/DestConfig via from_xxx() helpers below.
    """

    host: str
    username: str = ""
    password: str = ""
    nginx_mode: Literal["basic", "form_post", "off"] = "off"
    nginx_url: str = ""
    nginx_user_field: str = "username"
    nginx_pass_field: str = "password"
    nginx_extra_fields: dict[str, str] = {}

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
    ssh_port: int = 22
    ssh_user: str = "deluge"
    # Plain password auth (used only when no key file is provided).
    ssh_password: str = ""
    # Public-key auth
    ssh_key_path: Path | None = None
    ssh_key_passphrase: str = ""
    state_dir: Path

    @field_validator("ssh_key_path", mode="before")
    @classmethod
    def _empty_path_is_none(cls, v: object) -> object:
        """TOML "" coerces to Path(".") which is truthy; treat as None."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @model_validator(mode="after")
    def _check_auth(self) -> "DelugeSFTPConfig":
        # Only enforce credentials when the section is actually in use.
        if self.enabled and not self.ssh_key_path and not self.ssh_password:
            raise ValueError(
                "Deluge SFTP requires ssh_password or ssh_key_path when enabled"
            )
        return self


class SourceConfig(BaseModel):
    type: Literal["qbittorrent", "deluge"]
    host: str
    username: str = ""
    password: str = ""
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
    # client) before it is eligible for sync. Protects against
    # accidentally re-syncing long-running seed torrents when the
    # category filter is empty (i.e. sync-all mode).
    #
    # 0 (default) — sync every torrent the racing client has, regardless
    #   of age. Use this when the racing client doesn't categorise its
    #   torrents and you genuinely want everything.
    #
    # 3600 (1 hour) — sync torrents that were added in the last hour.
    #   Safer default if you ever change `category` away from "".
    #
    # 86400 (24 h) — only sync torrents added within the last day.
    min_age_seconds: int = Field(default=0, ge=0)
    # Optional nginx basic-auth in front of the WebUI. Leave url empty if
    # nginx is not used.
    nginx: NginxAuthConfig = NginxAuthConfig()
    deluge_sftp: DelugeSFTPConfig | None = None

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
    password: str = ""
    save_path: Path
    # Optional nginx basic-auth in front of the qBittorrent WebUI on VPS2.
    nginx: NginxAuthConfig = NginxAuthConfig()


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
        if not re.match(r"^[A-Za-z0-9_\-]+:", v):
            raise ValueError(
                f"rclone remote path must be of form 'name:path', got: {v!r}"
            )
        if not v.endswith("/"):
            raise ValueError(f"rclone remote path must end with '/', got: {v!r}")
        return v


class FuseConfig(BaseModel):
    mount: Path
    mount_unsorted: Path


class RcloneConfig(BaseModel):
    binary: Path = Path("/usr/bin/rclone")
    config_path: Path | None = None
    remote: RemoteConfig
    fuse: FuseConfig
    extra_move_flags: list[str] = Field(default_factory=list)
    batch_move_extra_flags: list[str] = Field(default_factory=list)


class ClassifierConfig(BaseModel):
    episode_regex: str = r"(?i)\bS\d{1,2}E\d{1,2}\b"
    _episode_re: re.Pattern[str] = re.compile(r"(?i)\bS\d{1,2}E\d{1,2}\b")

    @field_validator("episode_regex")
    @classmethod
    def _valid_regex(cls, v: str) -> str:
        re.compile(v)  # raises if invalid
        return v


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
    bot_token: str = ""
    chat_id: str = ""
    status_update_interval: int = Field(default=45, ge=5)
    pin_status_message: bool = False
    # Outbound rate limit for per-torrent detail messages (and
    # anything else we send). Telegram's bot API allows roughly 30
    # requests/sec across all chats. We self-throttle to `outbound_rate`
    # per second so a burst of state transitions (e.g. first run on a
    # racing client with 60+ torrents) doesn't trigger HTTP 429.
    outbound_rate: int = Field(default=5, ge=1)


class LoggingSinkConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    auth_token: str = ""
    forward_min_level: Literal[
        "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    ] = "INFO"


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
                return data
            # Otherwise the input is the flat dict.
            return {"entries": {str(k): str(v) for k, v in data.items()}}
        return data

    def resolve(self, announce_url: str) -> str | None:
        if not announce_url:
            return None
        low = announce_url.lower()
        for sub, name in self.entries.items():
            if sub.lower() in low:
                return name
        return None


class ProwlarrConfig(BaseModel):
    """req #5 + #6: Prowlarr integration."""

    enabled: bool = False
    base_url: str = ""       # e.g. http://127.0.0.1:9696
    api_key: str = ""
    # req #6: the indexer used for SSD downloads + cross-seed searches.
    # No default — you MUST set this when [prowlarr].enabled = true,
    # because the name must match exactly what your Prowlarr instance
    # calls the indexer (it's case-sensitive).
    download_indexer: str = ""
    # Timeout for HTTP calls to prowlarr
    timeout_seconds: float = 30.0
    # How many results to consider from a search
    max_results: int = 20
    # Tracker substring → indexer name map
    tracker_map: ProwlarrTrackerMap = ProwlarrTrackerMap()

    @model_validator(mode="after")
    def _validate(self) -> "ProwlarrConfig":
        if self.enabled:
            if not self.base_url.startswith(("http://", "https://")):
                raise ValueError("prowlarr.base_url must start with http(s)://")
            if not self.api_key:
                raise ValueError("prowlarr.api_key required when enabled")
            if not self.download_indexer:
                raise ValueError(
                    "prowlarr.download_indexer required when enabled. "
                    "Set it to the exact name of the indexer in your "
                    "Prowlarr instance (case-sensitive)."
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


class RecoveryConfig(BaseModel):
    """req #4: on startup, reconcile qBittorrent on VPS2 with state DB
    and filesystem (SSD + fuse)."""

    # Run reconciliation on startup
    run_on_startup: bool = True
    # Allowed transition states when reconciling (anything else = force fix)
    auto_fix_state: bool = True
    # How many torrents to inspect per pass (avoid hammering API)
    batch_size: int = 50
    # Reset previously-FAILED rows back to NEW on startup so they
    # get re-processed with the current code. Useful after a fix that
    # would have prevented the failure in the first place (e.g. a
    # new Deluge RPC fallback, a credentials change, etc.).
    # Set false if you want FAILED rows to stay failed for manual
    # intervention.
    auto_retry_failed: bool = True


class APIConfig(BaseModel):
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8765
    trust_nginx_header: bool = True
    api_token: str = ""


class GeneralConfig(BaseModel):
    source_poll_interval: int = Field(default=30, ge=5)
    dest_poll_interval: int = Field(default=15, ge=5)
    state_db: Path = Path("/var/lib/racing-sync/state.db")
    log_dir: Path = Path("/var/log/racing-sync")
    log_retention_days: int = Field(default=14, ge=1)
    disk_safety_margin_bytes: int = Field(default=0, ge=0)


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

    @classmethod
    def from_toml(cls, path: str | Path) -> "AppConfig":
        import tomllib

        with open(path, "rb") as f:
            data = tomllib.load(f)
        return cls.model_validate(data)

    def is_episode(self, name: str) -> bool:
        return bool(self.classifier._episode_re.search(name))