"""Typed configuration schema.

Loads from TOML and validates cross-field constraints (e.g. Deluge source
requires SFTP, rclone remote paths must be `name:path/`).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

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
    # HTTP pool shape (previously hardcoded in http_base): connection
    # pool size, per-host connections, DNS cache TTL seconds.
    pool_limit: int = Field(default=100, ge=1, le=1000)
    pool_per_host: int = Field(default=20, ge=1, le=200)
    dns_cache_ttl_seconds: int = Field(default=300, ge=0, le=86400)

    def has_nginx(self) -> bool:
        return self.nginx_mode in ("basic", "form_post")

    @classmethod
    def from_source(cls, src: SourceConfig) -> HTTPClientConfig:
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
    def from_dest(cls, dst: DestConfig) -> HTTPClientConfig:
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
    state_dir: Path | None = None
    # Independent SSH/SFTP connections in the exporter pool. Re-inject
    # bursts from concurrent workers used to serialize on one transport
    # (15s-timeout clusters); 3 lets a few fetches fly in parallel while
    # a wedged member fails over instead of wedging everyone.
    pool_size: int = Field(default=3, ge=1, le=8)

    @field_validator("ssh_key_path", "known_hosts_path", "state_dir", mode="before")
    @classmethod
    def _empty_path_is_none(cls, v: object) -> object:
        """TOML "" coerces to Path(".") which is truthy; treat as None."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @model_validator(mode="after")
    def _check_auth(self) -> DelugeSFTPConfig:
        # Only enforce credentials when the section is actually in use.
        has_pwd = bool(
            self.ssh_password.get_secret_value().strip()
            if hasattr(self.ssh_password, "get_secret_value")
            else str(self.ssh_password).strip()
        )
        if self.enabled and not self.ssh_key_path and not has_pwd:
            raise ValueError("Deluge SFTP requires ssh_password or ssh_key_path when enabled")
        if self.enabled and self.state_dir is None:
            raise ValueError("Deluge SFTP requires state_dir when enabled")
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
    #   category = "auto*"      # qBittorrent wildcard (single value)
    #
    # One value only: the client receives it verbatim as the category /
    # label filter, so a comma list would match nothing. Deluge uses the
    # term "label" but the field is unified under `category` for
    # simplicity; the coordinator calls the appropriate filter parameter
    # on each client.
    category: str = ""

    @field_validator("category")
    @classmethod
    def _category_is_a_single_name(cls, v: str) -> str:
        if "," in (v or ""):
            raise ValueError(
                "source.category takes ONE category/label — the client receives "
                "it verbatim, so a comma list would match nothing (an empty "
                "category syncs every torrent; qBittorrent also accepts a "
                "single wildcard like 'auto*')"
            )
        return v

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
    def _deluge_needs_sftp(self) -> SourceConfig:
        if self.type == "deluge" and (self.deluge_sftp is None or not self.deluge_sftp.enabled):
            raise ValueError("Deluge source requires [source.deluge_sftp] enabled")
        return self

    @model_validator(mode="after")
    def _check_host(self) -> SourceConfig:
        """Fail fast on unusable WebUI origins (not at first RPC)."""
        host = (self.host or "").strip()
        if not host.startswith(("http://", "https://")):
            raise ValueError(f"source.host must start with http(s):// (got: {host!r})")
        try:
            from urllib.parse import urlsplit as _us

            _p = _us(host)
            if _p.username or _p.password:
                raise ValueError(
                    "source.host must not embed userinfo "
                    "(creds would leak to logs — use username/password)"
                )
            if _p.query or _p.fragment:
                raise ValueError("source.host must be a bare origin (no query/fragment)")
            if not _p.hostname:
                raise ValueError("source.host must include a host")
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"source.host is not a valid URL: {e}") from None
        for label, secret in (
            ("username", self.username),
            (
                "password",
                self.password.get_secret_value()
                if isinstance(self.password, SecretStr)
                else str(self.password or ""),
            ),
        ):
            try:
                text = str(secret or "").strip()
            except Exception:
                continue
            if text and text.upper() in ("CHANGE_ME", "YOUR_USERNAME", "YOUR_PASSWORD"):
                raise ValueError(f"source.{label} looks like an unedited placeholder ({text!r})")
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

    @model_validator(mode="after")
    def _check_host_and_path(self) -> DestConfig:
        """Fail fast on an unusable WebUI origin or save_path.

        Otherwise these surface only later: a relative save_path is
        resolved against qBittorrent's own cwd, and a scheme/userinfo
        mistake shows up as an unrelated connection error.
        """
        host = (self.host or "").strip()
        if not host.startswith(("http://", "https://")):
            raise ValueError(f"dest.host must start with http(s):// (got: {host!r})")
        try:
            from urllib.parse import urlsplit as _us

            _p = _us(host)
            if _p.username or _p.password:
                raise ValueError(
                    "dest.host must not embed userinfo "
                    "(creds would leak to logs — use username/password)"
                )
            if _p.query or _p.fragment:
                raise ValueError("dest.host must be a bare origin (no query/fragment)")
            if not _p.hostname:
                raise ValueError("dest.host must include a host")
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"dest.host is not a valid URL: {e}") from None
        # save_path is a path on the *remote* client: a POSIX root is
        # absolute there even when this process runs on Windows.
        try:
            _sp = str(self.save_path).replace("\\", "/")
            if not (_sp.startswith("/") or self.save_path.is_absolute()):
                raise ValueError(
                    "dest.save_path must be absolute — qBittorrent resolves a "
                    f"relative path against its own cwd (got: {self.save_path})"
                )
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"dest.save_path is not a valid path: {e}") from None
        return self


class SSDConfig(BaseModel):
    path: Path
    # Upper bound (1 PiB): no SSD is that large — anything above is a
    # typo'd zero away from silently admitting the whole disk. The live
    # disk-total check in check-config still gates realistic values.
    max_inflight_bytes: int = Field(ge=1, le=2**50)
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
            raise ValueError(f"rclone remote path must be of form 'name:path', got: {v!r}")
        name, path = m.group(1), m.group(2)
        # Reject Windows drive letters (C:/, C:\) mistaken for remotes — and
        # drive-relative paths (C:foo), which rclone would treat as local
        # paths and misdirect moves.
        if len(name) == 1 and path.startswith(("/", "\\")):
            raise ValueError(f"rclone remote path looks like a Windows drive, got: {v!r}")
        if len(name) == 1 and not path.startswith("/"):
            raise ValueError(
                f"single-character rclone remote names are ambiguous with "
                f"Windows drive letters; rename the remote (got: {v!r})"
            )
        if not path or path.strip() in ("", "/"):
            raise ValueError(f"rclone remote path must include a path after 'name:', got: {v!r}")
        # NOTE: a leading '/' after the colon (`remote:/media/`) is valid
        # rclone addressing the remote root — only `..` escapes are refused.
        if ".." in [seg for seg in path.replace("\\", "/").split("/")]:
            raise ValueError(
                f"rclone remote path must not contain '..' segments "
                f"(escape from the remote root, got: {v!r})"
            )
        if not v.endswith("/"):
            raise ValueError(f"rclone remote path must end with '/', got: {v!r}")
        return v

    @model_validator(mode="after")
    def _no_shared_tree(self) -> RemoteConfig:
        """default and unsorted must not be the same tree.

        The identical tree for both makes moves land inside their own
        source filter — silent data loops. Sibling subtrees under one
        remote (`remote:qb/` + `remote:qb/unsorted/`) are the documented
        layout and stay allowed.
        """

        def _parts(v: str) -> tuple[str, str]:
            m = re.match(r"^([A-Za-z0-9_\-]+):(.*)$", (v or "").strip())
            if not m:
                return "", ""
            return m.group(1), m.group(2).rstrip("/")

        d_name, d_path = _parts(self.default)
        u_name, u_path = _parts(self.unsorted)
        if d_name and d_name == u_name and d_path == u_path:
            raise ValueError(
                f"rclone remotes default ({self.default!r}) and unsorted "
                f"({self.unsorted!r}) share the same tree — moves would land "
                "inside their own source; use disjoint paths"
            )
        return self


#: episode_regex patterns already proven safe by the backtracking probe
#: (per process — config loads repeat the default constantly).
_REDOS_PROBED_SAFE: dict[str, None] = {}


def _redos_probe(pattern: str) -> None:
    """Reject catastrophic-backtracking regexes via a subprocess probe.

    In-process timeouts cannot stop C-level `re` backtracking (no kill
    switch; a wedged thread holds the GIL), so the adversarial inputs run
    in a child interpreter with a hard timeout. Safe patterns finish in
    milliseconds; evil ones are killed at the deadline. Results cache per
    pattern — the default probes once per process. Raises ValueError on
    evil; probe-infra failures pass open (length + compile guards remain).
    """
    if pattern in _REDOS_PROBED_SAFE:
        return
    # NOTE: runs must be lowercase-consumable: backtracking only explodes
    # when the atoms actually match (uppercase runs fail instantly).
    # Evils travel via argv and stay small (~4KB total): Windows caps
    # command lines (~32K; long argv fails the spawn with WinError 206),
    # and stdin piping deadlocks against an unresponsive child. A 200-run
    # already means 2^200 paths for a nested quantifier — plenty.
    _evil_inputs = (
        "a" * 200 + "!",
        "a." * 250,
        " " * 1000 + "!",
        "[FL] " * 250,
        "S01E01" * 150 + "!",
        "a" * 150 + "b" * 150 + "!",
    )
    _prog = "import re, sys;rx = re.compile(sys.argv[1]);[rx.search(x) for x in sys.argv[2:]]"
    try:
        import subprocess as _sp
        import sys as _sys

        _res = _sp.run(
            [_sys.executable, "-c", _prog, pattern, *_evil_inputs], timeout=5.0, capture_output=True
        )
        if _res.returncode != 0:
            raise ValueError(
                "episode_regex probe errored in the worker "
                f"(rc={_res.returncode}): cannot prove it safe"
            )
    except _sp.TimeoutExpired:
        raise ValueError(
            "episode_regex did not finish the backtracking probe "
            "within 5s — likely catastrophic backtracking (ReDoS); "
            "simplify nested quantifiers"
        ) from None
    except ValueError:
        raise
    except Exception:
        pass
    _REDOS_PROBED_SAFE[pattern] = None


#: Exact-match rclone flags that hijack config/credentials/filters.
#: Shared with rclone_ops._reject_hijack_flags (runtime defense in depth)
#: so the two gates cannot drift.
_RCLONE_HIJACK_EXACT = frozenset(
    {
        "config",
        "password-command",
        "ask-password",
        "config-file",
        "c",
        "log-file",
        "log-file-max-size",
        "syslog",
        "syslog-facility",
        "password",
        "s3-access-key-id",
        "s3-secret-access-key",
        "s3-session-token",
        "drive-client-id",
        "drive-client-secret",
        "drive-token",
        "gcs-client-id",
        "gcs-client-secret",
        "gcs-token",
        "b2-account",
        "b2-key",
        "dropbox-client-id",
        "dropbox-client-secret",
        "dropbox-token",
        "onedrive-client-id",
        "onedrive-client-secret",
        "onedrive-token",
        "box-client-id",
        "box-client-secret",
        "box-token",
        "files-from",
        "files-from-raw",
        "files-from-encoding",
        "filter-from",
        "exclude-file",
        "include-file",
    }
)


def _is_rclone_hijack_flag(item: object) -> bool:
    """True when an rclone flag smuggles config/credentials/remote-control.

    Normalizes dash count and underscores like the runtime guard, then
    matches exact credential/config/filter flags plus whole families:
    every `--rc*` flag (remote-control surface) and every provider
    `--*-token` / `--*-client-id` / `--*-client-secret` flag. Tuning
    flags (--transfers, --bwlimit, --s3-chunk-size, ...) never match.
    """
    try:
        norm = str(item).strip().lower().split("=", 1)[0].lstrip("-").replace("_", "-")
    except Exception:
        return False
    if not norm:
        return False
    if norm in _RCLONE_HIJACK_EXACT:
        return True
    if norm == "rc" or norm.startswith("rc-"):
        return True
    if norm.endswith("-token") or norm.endswith("-client-id") or norm.endswith("-client-secret"):
        return True
    return False


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
    # Sustained-missing window (seconds) before the RE_ADDING healer
    # re-downloads fuse-gated files (default: 7200 / 2h). The fuse gate
    # parks on ANY absence, but rclone dir-cache (~5m) + app stat cache
    # (60s) + mount indexing lag make fresh absences unreliable — only a
    # *stable* missing set (same files, mount healthy, SSD bytes also
    # absent) older than this triggers a selective re-download + move of
    # just those files. 0 disables (park until the 24h max-age FAIL, then
    # auto-retry does a full re-download).
    readd_heal_missing_after_seconds: int = Field(default=7200, ge=0)
    # Manual-move wait (pre-pick): the operator rclone-moves a completed
    # torrent's files to the remote by hand while racing-sync is offline,
    # then starts the daemon. The VPS2 client has no entry, so the
    # exact-infohash adoption misses and the row would otherwise query
    # Prowlarr / reserve SSD budget for content that already sits on the
    # fuse mount. When the expected files are *partly* there (a move in
    # progress, or fuse index lag) the row parks instead of downloading
    # and re-checks every `manual_move_check_seconds` (default: 1800 /
    # 30 min) for up to `manual_move_timeout_seconds` (default: 43200 /
    # 12 hours), then falls back to the normal SSD download. 0 disables
    # the wait entirely (pick immediately).
    manual_move_check_seconds: int = Field(default=1800, ge=60)
    manual_move_timeout_seconds: int = Field(default=43200, ge=0)

    @model_validator(mode="after")
    def _manual_move_window_consistent(self) -> FuseConfig:
        """A wait window shorter than one check interval never re-checks."""
        if (
            self.manual_move_timeout_seconds > 0
            and self.manual_move_timeout_seconds < self.manual_move_check_seconds
        ):
            raise ValueError(
                "manual_move_timeout_seconds "
                f"({self.manual_move_timeout_seconds}) must be 0 (disabled) or "
                f">= manual_move_check_seconds ({self.manual_move_check_seconds})"
            )
        return self


class RcloneConfig(BaseModel):
    binary: Path = Path("/usr/local/bin/rclone")
    config_path: Path | None = None
    #: None when the transfer link is disabled (no remote destination).
    remote: RemoteConfig | None = None
    #: None when the seed link is disabled (no fuse/plain seed path).
    fuse: FuseConfig | None = None
    extra_move_flags: list[str] = Field(default_factory=list)
    batch_move_extra_flags: list[str] = Field(default_factory=list)
    # Maximum concurrent rclone move commands running simultaneously (default: 3)
    max_concurrent_moves: int = Field(default=3, ge=1, le=100)
    # Wall-clock ceiling per `rclone move` (seconds). A remote that stops
    # responding (e.g. API flood-wait pileup on Telegram-backed remotes)
    # must park the row for retry, never wedge a move slot forever. 6h
    # preserves the historical default; lower it on fast remotes (e.g.
    # 1800) so stalls surface sooner. Timeouts park (bytes stay on SSD),
    # never fail the row.
    move_timeout_seconds: float = Field(default=6 * 3600, ge=60)
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
        prompts; credential-bearing provider flags and the `--rc*`
        remote-control surface are refused too. Everything else
        (transfers, bwlimit, s3-chunk-size, ...) passes through. The
        runtime guard (rclone_ops) shares the predicate.
        """
        bad: list[str] = []
        for item in v or []:
            try:
                if _is_rclone_hijack_flag(item):
                    bad.append(str(item))
            except Exception:
                continue
        if bad:
            raise ValueError(
                "rclone move flags must not include config/credential/filter-hijack "
                "flags (--config[-file], --password-command, --ask-password, "
                "--password, --rc*, --*-token/--*-client-id/--*-client-secret, "
                "--s3-access-key-id, --files-from*, "
                "--filter-from, --exclude/include-file), got: "
                f"{bad!r}"
            )
        return v


class ClassifierConfig(BaseModel):
    # SxxEyy (1-3 digits each side) plus the anime scene "Title - NN (…)"
    # single-episode form (dash + number + opening paren, e.g. a fansub
    # "Show - 10 (1080p) [HASH].mkv"). The trailing paren is what keeps
    # batch ranges ("(01-13) (1080p) [Batch]", no dash-number) and bare
    # titles from matching as episodes. Numbers over 3 digits stay out,
    # so year-titled movies ("Film - 2019 (2024)") don't match either.
    episode_regex: str = r"(?i)(?:\bS\d{1,3}E\d{1,3}\b| - (\d{1,3}) \()"

    @field_validator("episode_regex")
    @classmethod
    def _valid_regex(cls, v: str) -> str:
        if len(v) > 500:
            raise ValueError("episode_regex exceeds 500 chars (ReDoS guard)")
        re.compile(v)  # raises if invalid
        # Backtracking probe: adversarial inputs must match fast.
        _redos_probe(v)
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
    # Destructive-command allowlist (Telegram user ids). Empty (default)
    # preserves today's behavior: anyone in the authorized chat may run
    # /cancel and tap the buttons. Non-empty restricts
    # commands AND button taps to these users; other chat members get a
    # "not authorized" reply. Pending flows additionally bind to the
    # commanding user, so one operator cannot tap another's picker.
    admin_user_ids: list[int] = Field(default_factory=list)
    status_update_interval: int = Field(default=45, ge=5)
    pin_status_message: bool = False
    # Outbound rate limit for per-torrent detail messages (and
    # anything else we send). Telegram's bot API allows roughly 30
    # requests/sec across all chats. We self-throttle to `outbound_rate`
    # per second so a burst of state transitions (e.g. first run on a
    # racing client with 60+ torrents) doesn't trigger HTTP 429.
    outbound_rate: int = Field(default=5, ge=1, le=30)
    # Number of active tasks displayed per page in the status message (default: 5)
    page_size: int = Field(default=5, ge=1, le=50)
    # Re-post (delete + resend, silent) the active-tasks message when our
    # own newer traffic has buried it, at most every N seconds, so it
    # returns to newest-message position. Never fires while already last.
    # 0 = disabled (edit in place, today's behavior). Values 1-4 are
    # clamped to 5 to respect Telegram rate limits.
    active_repost_interval_seconds: int = Field(default=0, ge=0)
    # Delete an ingested .torrent file message from chat once /add has
    # consumed it (default true). .torrent files embed the sender's
    # per-user tracker passkey — leaving them in chat history leaks it to
    # every current and future chat member. Needs message-deletion rights
    # (admin in groups); without them the bot warns and the operator
    # removes the file by hand. False keeps the file message in place.
    delete_processed_torrent: bool = True

    @model_validator(mode="after")
    def _validate(self) -> TelegramConfig:
        if self.enabled:
            token = self.bot_token.get_secret_value().strip()
            if not token or token.upper() in ("CHANGE_ME", "YOUR_BOT_TOKEN"):
                raise ValueError(
                    "telegram.bot_token is required and cannot be a placeholder when enabled"
                )
            chat = self.chat_id.strip()
            if not chat or chat.upper() in ("CHANGE_ME", "YOUR_CHAT_ID"):
                raise ValueError(
                    "telegram.chat_id is required and cannot be a placeholder when enabled"
                )
        # Fail at startup, not at first tap: a bad allowlist entry makes
        # _tg_actor_allowed fail closed for everyone (int() raises), so
        # validate here. Telegram user ids are positive; duplicates are
        # harmless but normalized away.
        seen: list[int] = []
        for uid in self.admin_user_ids:
            if not isinstance(uid, int) or isinstance(uid, bool) or uid <= 0:
                raise ValueError("telegram.admin_user_ids must hold positive Telegram user ids")
            if uid not in seen:
                seen.append(uid)
        self.admin_user_ids = seen
        if self.enabled:
            try:
                _product = int(self.outbound_rate or 0) * int(self.status_update_interval or 0)
            except (TypeError, ValueError):
                _product = 0
            if _product > 100:
                import logging as _logging

                _logging.getLogger(__name__).warning(
                    "telegram: outbound_rate (%d) × status_update_interval (%ds) = %d "
                    "edits per pinned-message cycle — expect Telegram 429 "
                    "flood-control; lower the rate or lengthen the interval",
                    self.outbound_rate,
                    self.status_update_interval,
                    _product,
                )
            try:
                _repost = int(self.active_repost_interval_seconds or 0)
            except (TypeError, ValueError):
                _repost = 0
            if 1 <= _repost <= 4:
                import logging as _logging

                _logging.getLogger(__name__).warning(
                    "telegram: active_repost_interval_seconds=%d is clamped to 5 "
                    "at runtime to respect rate limits; set 0 (off) or >= 5",
                    _repost,
                )
        return self


class LoggingSinkConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    auth_token: SecretStr = SecretStr("")
    forward_min_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # Plaintext http:// to a non-local host ships log bodies unencrypted:
    # fail closed unless the operator explicitly opts in.
    allow_plaintext: bool = False

    @model_validator(mode="after")
    def _validate(self) -> LoggingSinkConfig:
        if self.enabled:
            if not self.url or not self.url.strip().startswith(("http://", "https://")):
                raise ValueError("logging_sink.url must be a valid http(s) URL when enabled")
            try:
                from urllib.parse import urlsplit as _split

                parts = _split(self.url.strip())
                if (
                    parts.scheme == "http"
                    and parts.hostname not in ("localhost", "127.0.0.1", "::1")
                    and not self.allow_plaintext
                ):
                    raise ValueError(
                        "logging_sink.url is plaintext http:// to a non-local host: "
                        "log bodies would ship unencrypted — use https:// or set "
                        "logging_sink.allow_plaintext=true to confirm"
                    )
            except ValueError:
                raise
            except Exception:
                pass
        return self


class WatchDirConfig(BaseModel):
    """req #3: manual private torrents dropped here are processed."""

    path: Path
    # Glob of filenames to accept (matched case-insensitively for the
    # .torrent suffix: Show.TORRENT is picked up too). Anything else is ignored.
    glob: str = "*.torrent"
    # If true, delete the .torrent after it has been picked up.
    delete_after_pickup: bool = True
    # Prowlarr query policy for watch-dir drops
    query_prowlarr: bool = True
    # If prowlarr returns a hit on the configured indexer, prefer that
    # torrent over the manually-dropped file.
    prefer_prowlarr_result: bool = True

    @field_validator("glob")
    @classmethod
    def _glob_must_stay_inside(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("watch_dir.glob must not be empty")
        norm = v.strip().replace("\\", "/")
        if norm.startswith("/") or ".." in norm.split("/") or ":" in norm:
            raise ValueError(
                f"watch_dir.glob must be a plain filename pattern inside the "
                f"watch dir (no paths/escapes), got: {v!r}"
            )
        return v


class ProwlarrTrackerMap(BaseModel):
    """req #4: announce URL substring → prowlarr indexer name.

    The TOML shape is a flat dict under `[prowlarr.tracker_map]`, e.g.:

        [prowlarr.tracker_map]
        "alpha.cc"      = "Alpha (API)"
        "beta"      = "Beta"
        "gamma.tv"  = "Gamma"

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
        - a flat dict:        {"alpha.cc": "Alpha (API)"}
        - the existing shape: {"entries": {...}}
        """
        if isinstance(data, dict):
            # Detect the OLD schema (which had `_substr` / `_index`
            # fields or a nested `overrides` table) and fail loudly so
            # users get a clear migration error instead of silently
            # broken cross-seed matching.
            for forbidden in (
                "overrides",
                "beta_substr",
                "beta_index",
                "alpha_substr",
                "alpha_index",
                "gamma_substr",
                "gamma_index",
            ):
                if forbidden in data:
                    raise ValueError(
                        f"[prowlarr.tracker_map] uses the old schema. "
                        f"`{forbidden}` is no longer supported. "
                        f"Use a flat dict instead, e.g.:\n"
                        f"  [prowlarr.tracker_map]\n"
                        f'  "alpha.cc" = "Alpha (API)"\n'
                        f'  "beta" = "Beta"'
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
                if len(str(k).strip()) < 2:
                    raise ValueError(
                        f"[prowlarr.tracker_map] substring key {k!r} is too short "
                        "(single characters match nearly every announce URL)"
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


class DownloadIndexerConfig(BaseModel):
    """One SSD download-target indexer, in priority order (first = tried first).

    TOML shape — repeat the table per indexer:

        [[prowlarr.download_indexers]]
        name = "My Indexer (API)"
        announce_substrings = ["my-indexer"]

        [[prowlarr.download_indexers]]
        name = "Other Indexer (API)"
        announce_substrings = ["other"]

    `name` must match the indexer's EXACT name in Prowlarr
    (case-sensitive). `announce_substrings` identifies torrents that
    already come from this indexer (announce URL substring match), so a
    watch-dir drop from it is used directly instead of being
    re-searched (sacrificial-copy detection).
    """

    name: str = ""
    announce_substrings: list[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _name_non_empty(cls, v: str) -> str:
        if not str(v).strip():
            raise ValueError(
                "prowlarr download indexer entry has an empty name "
                "(must match the indexer name in Prowlarr exactly)"
            )
        return str(v).strip()


class ProwlarrConfig(BaseModel):
    """req #5 + #6: Prowlarr integration."""

    enabled: bool = False
    base_url: str = ""  # e.g. http://127.0.0.1:9696
    api_key: SecretStr = SecretStr("")
    # req #6: ordered SSD download-target indexers used for SSD downloads +
    # cross-seed searches (first entry = highest priority; each is tried in
    # turn until one returns the exact release). Configure via
    # `[[prowlarr.download_indexers]]` tables (see DownloadIndexerConfig).
    download_indexers: list[DownloadIndexerConfig] = Field(default_factory=list)
    # Timeout for HTTP calls to prowlarr
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    # How many results to consider from a search
    max_results: int = Field(default=20, ge=1, le=1000)
    # Tracker substring → indexer name map
    tracker_map: ProwlarrTrackerMap = ProwlarrTrackerMap()
    # Substrings in torrent titles to skip querying Prowlarr for (case-insensitive)
    skip_query_substrings: list[str] = Field(default_factory=list)
    # Network family for the Prowlarr session. False (default) forces IPv4:
    # intentional — see [source].use_ipv6.
    use_ipv6: bool = False
    # HTTP pool shape + parallel-search bounds (previously hardcoded in
    # the client): connection pool size, per-host connections, DNS cache
    # TTL seconds, fan-out concurrency and overall gather timeout.
    pool_limit: int = Field(default=100, ge=1, le=1000)
    pool_per_host: int = Field(default=20, ge=1, le=200)
    dns_cache_ttl_seconds: int = Field(default=300, ge=0, le=86400)
    parallel_search_concurrency: int = Field(default=4, ge=1, le=16)
    parallel_search_timeout_seconds: float = Field(default=90.0, ge=5.0, le=600.0)

    @property
    def download_indexer_names(self) -> list[str]:
        """Configured download-target indexer names, in priority order."""
        return [e.name for e in self.download_indexers]

    def is_download_indexer(self, announce_url: str) -> bool:
        """Check if an announce URL belongs to any download-target indexer."""
        if not announce_url:
            return False
        low = announce_url.lower()
        for entry in self.download_indexers:
            for sub in entry.announce_substrings:
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
    def _validate(self) -> ProwlarrConfig:
        entries = list(self.download_indexers or [])
        seen: set[str] = set()
        for entry in entries:
            low = entry.name.lower()
            if low in seen:
                raise ValueError(
                    f"prowlarr download indexer {entry.name!r} is listed twice "
                    "(names must be unique, case-insensitive)"
                )
            seen.add(low)
            for sub in entry.announce_substrings:
                if not str(sub).strip():
                    raise ValueError(
                        f"prowlarr download indexer {entry.name!r} has a blank "
                        "announce_substrings entry (blank matches every URL)"
                    )
        for sub in self.skip_query_substrings or []:
            if len(str(sub).strip()) < 2:
                raise ValueError(
                    f"prowlarr skip_query_substrings entry {sub!r} is too short "
                    "(single characters match nearly every title — same rule "
                    "as tracker_map substring keys)"
                )
        self.download_indexers = entries
        if self.enabled:
            if not self.base_url.startswith(("http://", "https://")):
                raise ValueError("prowlarr.base_url must start with http(s)://")
            try:
                from urllib.parse import urlsplit as _us

                _p = _us(self.base_url)
                if _p.username or _p.password:
                    raise ValueError(
                        "prowlarr.base_url must not embed userinfo (creds would leak to logs)"
                    )
                if _p.query or _p.fragment:
                    raise ValueError("prowlarr.base_url must be a bare origin (no query/fragment)")
                if not _p.hostname:
                    raise ValueError("prowlarr.base_url must include a host")
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"prowlarr.base_url is not a valid URL: {e}") from None
            key = (
                self.api_key.get_secret_value().strip()
                if isinstance(self.api_key, SecretStr)
                else str(self.api_key).strip()
            )
            if not key or key.upper() in ("CHANGE_ME", "YOUR_PROWLARR_API_KEY"):
                raise ValueError(
                    "prowlarr.api_key required and cannot be a placeholder when enabled"
                )
            if not entries:
                raise ValueError(
                    "prowlarr enabled but no download-target indexer is configured. "
                    "Add at least one [[prowlarr.download_indexers]] table with "
                    "the exact indexer name from your Prowlarr instance "
                    "(case-sensitive)."
                )
            if not any(e.announce_substrings for e in entries):
                import logging as _logging

                _logging.getLogger(__name__).warning(
                    "prowlarr enabled but no download indexer has "
                    "announce_substrings — is_download_indexer() will always "
                    "return False and sacrificial-copy detection is disabled",
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
    # req #2 follow-up: if no download-target indexer returns a hit
    # (release too new), retry the query every
    # `prowlarr_retry_interval_seconds`. Give up after
    # `prowlarr_max_age_seconds` since the FIRST query attempt and mark
    # the torrent FAILED for manual handling.
    #
    # The names use "prowlarr_" because the search runs through Prowlarr;
    # the *indexers* queried are whatever the user configured under
    # [[prowlarr.download_indexers]], tried in priority order.
    prowlarr_retry_interval_seconds: int = Field(default=1800, ge=60)  # 30 min
    prowlarr_max_age_seconds: int = Field(default=86400, ge=3600)  # 24 h
    # VPS1 public-torrent export retry cadence: a public racing torrent
    # whose .torrent can't be exported (transient SFTP/export glitch)
    # retries on its own schedule with NO max-age clock — the source
    # torrent still exists on VPS1, so giving up after 24h would kill a
    # recoverable row. A vanished source still fails via the consecutive
    # source-miss guard, so this cannot spin forever on a dead torrent.
    public_export_retry_interval_seconds: int = Field(default=1800, ge=60)  # 30 min

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
    #   content, we query Prowlarr on the configured download-target
    #   indexers (in priority order) to obtain a .torrent
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
    # `pause_public_torrents_on_fuse` (default true):
    #   When true, public torrents injected onto the fuse mount are added
    #   paused (and an already-seeding fuse entry for one is paused), so
    #   operators that don't want to seed publics from the remote still keep
    #   the entry. Private torrents are never paused by this flag.
    #   Detection is best-effort from the .torrent announce URL: undecodable
    #   blobs fail open toward seeding (current behavior) so a private is
    #   never paused by mistake.
    #
    # Note: for Deluge sources, SFTP is the only way to obtain the .torrent
    # bytes for the racing client's own torrents. The coordinator will
    # automatically enable this and refuse to start if SFTP credentials
    # are missing.
    inject_racing_torrents_to_fuse: bool = True
    allow_prowlarr_cross_seed: bool = True
    allow_ssh_export: bool = True
    pause_public_torrents_on_fuse: bool = True
    # When true, a private torrent that still has no Prowlarr cross-seed
    # after prowlarr_max_age_seconds falls back to the racing client's own
    # .torrent (SFTP / export endpoint) for the SSD download instead of
    # FAILED. VPS2 then leeches the private swarm, which counts toward
    # ratio — opt in deliberately. Default false (fail for manual handling).
    # A Telegram /start_<hash> command triggers the same fallback on
    # demand for a WAITING_INDEXER row without needing this flag.
    fallback_to_racing_torrent_on_prowlarr_timeout: bool = False

    @model_validator(mode="after")
    def _check_strategy(self) -> CrossSeedConfig:
        if not self.allow_prowlarr_cross_seed and not self.allow_ssh_export:
            raise ValueError(
                "cross_seed: allow_prowlarr_cross_seed=false + allow_ssh_export=false "
                "leaves no SSD-source strategy for private torrents "
                "(every torrent would park in WAITING_INDEXER until FAILED). "
                "Enable at least one unless this is a public-only deployment."
            )
        if self.fallback_to_racing_torrent_on_prowlarr_timeout and not self.allow_ssh_export:
            raise ValueError(
                "cross_seed: fallback_to_racing_torrent_on_prowlarr_timeout=true "
                "requires allow_ssh_export=true (the fallback fetches the "
                "racing torrent's own bytes via SFTP / export endpoint)."
            )
        if int(self.prowlarr_retry_interval_seconds or 0) >= int(
            self.prowlarr_max_age_seconds or 0
        ):
            raise ValueError(
                "cross_seed: prowlarr_retry_interval_seconds "
                f"({self.prowlarr_retry_interval_seconds}) must be less than "
                f"prowlarr_max_age_seconds ({self.prowlarr_max_age_seconds}) — "
                "otherwise the retry clock never fires before the max-age FAIL"
            )
        if self.refetch_public_via_prowlarr and not self.allow_prowlarr_cross_seed:
            raise ValueError(
                "cross_seed: refetch_public_via_prowlarr=true with "
                "allow_prowlarr_cross_seed=false is a silent no-op (no Prowlarr "
                "query ever runs) — enable allow_prowlarr_cross_seed or drop "
                "the refetch flag"
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
    low_watermark_free_bytes: int = Field(default=16106127360, ge=0)  # 15 GiB
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
    # Settle pause (seconds) between real group deletions in one run.
    # Back-to-back remove_torrent(+data) calls spike daemon teardown and
    # disk unlinking, which has coincided with autobrr injection timeouts
    # on the same Deluge daemon. 0 = delete back-to-back (old behavior).
    delete_stagger_seconds: float = Field(default=25.0, ge=0)
    # Seconds between janitor runs.
    janitor_interval_seconds: int = Field(default=3600, ge=300)
    # Settle delay after (re)start before the first janitor run may delete.
    # A minutes-long reboot for an upgrade must not trigger an immediate
    # run: recovery, polls and SFTP settle first. 0 disables (run as soon
    # as the hourly cadence allows).
    janitor_startup_delay_seconds: float = Field(default=300.0, ge=0)

    @model_validator(mode="after")
    def _check_bounds(self) -> CleanupConfig:
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
            raise ValueError("cleanup: burst_arrivals_per_hour must be > calm_arrivals_per_hour")
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
    # Optional TLS termination (direct exposure). Both or neither; when
    # set, uvicorn serves HTTPS. Prefer a reverse proxy for WAN and keep
    # these empty — a token over plain HTTP on 0.0.0.0 is sniffable.
    tls_certfile: str = ""
    tls_keyfile: str = ""

    @model_validator(mode="after")
    def _validate(self) -> APIConfig:
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
            if token and token.upper() in (
                "CHANGE_ME",
                "YOUR_API_TOKEN",
                # The literal shipped in config.example.toml: public to
                # everyone who reads the repo, so never a real credential.
                "GENERATE_A_RANDOM_TOKEN_HERE",
            ):
                raise ValueError("api.api_token cannot be a placeholder when enabled")
            cert, key = (self.tls_certfile or "").strip(), (self.tls_keyfile or "").strip()
            if bool(cert) != bool(key):
                raise ValueError("api.tls_certfile and api.tls_keyfile must be set together")
            if self.enabled:
                for label, raw in (("tls_certfile", cert), ("tls_keyfile", key)):
                    if raw and not Path(raw).is_file():
                        raise ValueError(f"api.{label} does not exist: {raw}")
            if self.enabled and self.trust_nginx_header:
                # The header path trusts any listed proxy completely: a
                # world-range entry is a full auth bypass, and a hostname
                # never matches the IP literal the server actually sees
                # (dead entry that looks protective).
                import ipaddress as _ip

                for entry in list(self.trusted_proxies or []):
                    text = str(entry or "").strip().strip("[]")
                    if not text:
                        raise ValueError(
                            "api.trusted_proxies has a blank entry "
                            "(blank matches nothing; remove it)"
                        )
                    if text.lower() == "localhost":
                        continue
                    try:
                        if "/" in text:
                            net = _ip.ip_network(text, strict=False)
                        elif ":" in text:
                            net = _ip.ip_network(text + "/128", strict=False)
                        else:
                            net = _ip.ip_network(text + "/32", strict=False)
                    except ValueError:
                        raise ValueError(
                            f"api.trusted_proxies entry {entry!r} is not an "
                            f"IP, CIDR range, or 'localhost'"
                        ) from None
                    if net.prefixlen == 0:
                        raise ValueError(
                            f"api.trusted_proxies entry {entry!r} trusts the "
                            f"whole internet (prefix /0) — refusing"
                        )
        return self


class GeneralConfig(BaseModel):
    source_poll_interval: int = Field(default=30, ge=5)
    dest_poll_interval: int = Field(default=15, ge=5)
    # Per-RPC timeout budget (seconds) for client calls (qB/Deluge/fuse
    # stat, Prowlarr, SFTP): a hung peer degrades one operation into a
    # park-and-retry instead of wedging a worker or the tick forever.
    client_rpc_timeout_seconds: int = Field(default=30, ge=5, le=300)
    state_db: Path = Path("/var/lib/racing-sync/state.db")
    log_dir: Path = Path("/var/log/racing-sync")
    log_retention_days: int = Field(default=14, ge=1)
    disk_safety_margin_bytes: int = Field(default=0, ge=0)
    # Optional overrides if specified under [general]
    max_active_downloads: int | None = Field(default=None, ge=1, le=100)
    max_concurrent_moves: int | None = Field(default=None, ge=1, le=100)
    download_stall_timeout_seconds: int = Field(default=7200, ge=0)
    # Consecutive no-progress stall windows before a DOWNLOADING row is
    # failed terminally instead of parked again. A window with ANY byte of
    # progress resets the count, so slow-but-alive swarms are never failed
    # — only rows that never advance (tracker-deleted releases slip past
    # the tracker-message check, dead swarms) release their slot. 0/1 =
    # fail on the first stalled window; each window is stall_timeout long.
    download_max_stall_parks: int = Field(default=3, ge=0, le=100)
    # Max continuous client hash-check wait per MOVING row (seconds) before
    # failing terminally with a Telegram page. A check that never finishes
    # (wedged client, stalled storage) would otherwise park the row — and
    # its SSD bytes — forever. 0 = wait indefinitely (old behavior).
    download_max_check_wait_seconds: int = Field(default=43200, ge=0)
    # No-advance window inside a hash check (seconds): the client reports
    # verification progress, so a healthy-but-slow check keeps resetting
    # this while a wedged one trips it (default 1 hour, 0 disables, leaving
    # only the absolute cap above).
    download_check_stall_seconds: int = Field(default=3600, ge=0)
    # Preferred-copy grace (seconds): a NEW row whose source is private
    # and NOT from a download-target indexer holds (stays NEW) this long
    # before locking the download, giving a download-indexer copy of the
    # same content time to arrive and preempt it. Prevents burning ratio
    # on a non-preferred swarm when the preferred one shows up moments
    # later. 0 = disabled (today's immediate behavior).
    preferred_copy_grace_seconds: int = Field(default=3600, ge=0)


class PipelineConfig(BaseModel):
    """Which capability links this deployment runs.

    Set by `config_alias` from the `[acquire]`/`[transfer]`/`[seed]`
    switches; not part of the documented file surface yet. Disabling a
    link is only accepted when the pipeline can reach its terminal state
    without it (see `racing_sync.chain`).
    """

    #: Providers this release can run (files choose explicitly; `auto`
    #: keeps the legacy cross-seed/direct-export preference).
    acquire: Literal["auto", "local", "external"] = "auto"
    transfer: Literal["rclone", "local_move", "none", "external"] = "rclone"
    seed: Literal["path", "existing", "none"] = "path"
    #: local_move destination roots (absolute; disjoint from storage).
    local_path: Path | None = None
    local_unsorted: Path | None = None

    @field_validator("local_path", "local_unsorted", mode="before")
    @classmethod
    def _empty_path_is_none(cls, v: object) -> object:
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @model_validator(mode="after")
    def _local_move_needs_a_destination(self) -> PipelineConfig:
        if self.transfer != "local_move":
            return self
        if self.local_path is None:
            raise ValueError(
                '[transfer].path is required when provider = "local_move" '
                "(the local/NAS directory finished content is moved to)"
            )
        if self.local_unsorted is None:
            self.local_unsorted = Path(str(self.local_path).rstrip("/\\")) / "unsorted"
        for label, raw in (("path", self.local_path), ("unsorted", self.local_unsorted)):
            text = str(raw).replace("\\", "/")
            try:
                absolute = Path(str(raw)).is_absolute() or text.startswith("/")
            except Exception:
                absolute = False
            if not absolute:
                raise ValueError(f"[transfer].{label} must be an absolute local path, got: {raw}")
        return self


class AppConfig(BaseModel):
    general: GeneralConfig
    source: SourceConfig
    dest: DestConfig
    ssd: SSDConfig
    rclone: RcloneConfig = RcloneConfig()
    #: Capability links; internal, derived by the alias layer.
    pipeline: PipelineConfig = PipelineConfig()
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
    def from_toml(cls, path: str | Path) -> AppConfig:
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib  # type: ignore[import-not-found,no-redef]

        with open(path, "rb") as f:
            data = tomllib.load(f)
        # New capability-chain spellings resolve onto the internal schema
        # before validation, so every consumer keeps reading today's
        # sections. Deprecated keys still win; report each once per load.
        from .config_alias import normalize_config

        normalized = normalize_config(data)
        import logging as _logging

        _log = _logging.getLogger(__name__)
        for message in normalized.deprecations:
            _log.warning("config: %s", message)
        for key in normalized.unknown:
            _log.warning("config: unknown key %r — ignored (possible typo)", key)
        data = normalized.data
        _warn_unknown_keys(cls, data)
        # Secrets at rest: the file holds passwords/api keys/bot tokens —
        # warn when group/other can read it (0600 recommended). POSIX-only:
        # Windows ACLs don't map to mode bits, so the check is meaningless
        # (and noisy) there.
        try:
            import os as _os

            if _os.name == "posix":
                _mode = _os.stat(path).st_mode & 0o077
                if _mode:
                    import logging as _logging

                    _logging.getLogger(__name__).warning(
                        "config file %s is readable by group/other (mode %o); "
                        "consider chmod 600 — it holds secrets",
                        path,
                        _os.stat(path).st_mode & 0o777,
                    )
        except Exception:
            pass
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
        return self.rclone.fuse.reinject_delay_seconds if self.rclone.fuse else 30

    @property
    def fuse_reinject_retry_gap_seconds(self) -> int:
        return self.rclone.fuse.reinject_retry_gap_seconds if self.rclone.fuse else 120

    @property
    def fuse_reinject_backoff_seconds(self) -> int:
        return self.rclone.fuse.reinject_backoff_seconds if self.rclone.fuse else 1800

    @property
    def fuse_reinject_max_age_seconds(self) -> int:
        return self.rclone.fuse.reinject_max_age_seconds if self.rclone.fuse else 86400

    @property
    def fuse_readd_heal_after_seconds(self) -> int:
        return self.rclone.fuse.readd_heal_missing_after_seconds if self.rclone.fuse else 7200

    @property
    def fuse_manual_move_check_seconds(self) -> int:
        return self.rclone.fuse.manual_move_check_seconds if self.rclone.fuse else 1800

    @property
    def fuse_manual_move_timeout_seconds(self) -> int:
        return self.rclone.fuse.manual_move_timeout_seconds if self.rclone.fuse else 43200

    @model_validator(mode="after")
    def _links_need_their_config(self) -> AppConfig:
        """Links require the settings they run on (fail fast)."""
        link = self.pipeline
        external_acquire = link.acquire == "external"
        if link.acquire == "local":
            from .chain import same_client

            if not same_client(getattr(self.source, "host", ""), getattr(self.dest, "host", "")):
                raise ValueError(
                    "local acquisition needs [client] and [dest] on the same client "
                    "(same host:port) — the racing copy IS the staged copy. Use "
                    'provider = "auto" when the boxes are separate.'
                )
        if external_acquire and link.transfer == "rclone":
            raise ValueError(
                "external acquisition means the operator copies the bytes — "
                'set [transfer] enabled = false (or provider = "external") '
                "so the daemon does not download and move them instead"
            )
        if link.transfer == "rclone" and self.rclone.remote is None:
            raise ValueError(
                "the transfer link is enabled but [rclone.remote] is missing — "
                "set [remote] path/unsorted, or disable the link"
            )
        if link.seed == "path" and self.rclone.fuse is None:
            raise ValueError(
                "the seed link is enabled but no seed path is configured — "
                "set [seed] path, or disable the link"
            )
        if link.seed == "existing" and link.acquire != "external":
            raise ValueError(
                "seed provider 'existing' means the content is already at the "
                'seed path — set [acquire] provider = "external" (adopt-only) '
                "so nothing downloads a second copy"
            )
        if link.seed == "path" and link.transfer == "none" and not external_acquire:
            raise ValueError(
                "the seed link needs the transfer link: bytes cannot reach the "
                "seed path without a move. Disable both for a download-only "
                "chain, or keep transfer on (external acquisition is the "
                "exception: the operator copies the bytes)."
            )
        if external_acquire and link.transfer == "rclone":
            raise ValueError(
                "external acquisition means the operator copies the bytes — "
                'set [transfer] enabled = false (or provider = "external") '
                "so the daemon does not download and move them instead"
            )
        if external_acquire and link.seed == "none":
            raise ValueError(
                "external acquisition needs a seed path to verify the copy "
                'against — set [seed] path, or use [acquire] provider = "auto"'
            )
        if external_acquire and self.rclone.fuse is not None:
            fuse = self.rclone.fuse
            if int(getattr(fuse, "manual_move_timeout_seconds", 0) or 0) <= 0:
                raise ValueError(
                    "external acquisition waits for the operator's copy, so the "
                    "manual-move window must be enabled "
                    "(manual_move_timeout_seconds > 0)"
                )
        return self

    @model_validator(mode="after")
    def _warn_suspicious_combos(self) -> AppConfig:
        """Log suspicious cross-section combos (warn, never refuse).

        Refusing to start on a merely-slow combination would strand
        upgrades; a loud warning at load names the trade-off instead.
        """
        try:
            import logging as _logging

            _log = _logging.getLogger(__name__)
            try:
                _rpc = int(self.general.client_rpc_timeout_seconds or 0)
                _poll = min(
                    int(self.general.source_poll_interval or 0),
                    int(self.general.dest_poll_interval or 0),
                )
            except (TypeError, ValueError):
                _rpc, _poll = 0, 0
            if _rpc and _poll and _rpc > _poll:
                _log.warning(
                    "config: [general].client_rpc_timeout_seconds (%d) exceeds "
                    "the poll interval (%ds) — a hung RPC outlives its tick; "
                    "lower the timeout or raise the poll interval",
                    _rpc,
                    _poll,
                )
            try:
                if self.general.max_active_downloads is not None:
                    _log.info(
                        "config: [general].max_active_downloads=%d wins over "
                        "[dest].max_active_downloads=%d",
                        self.general.max_active_downloads,
                        self.dest.max_active_downloads,
                    )
                if self.general.max_concurrent_moves is not None:
                    _log.info(
                        "config: [general].max_concurrent_moves=%d wins over "
                        "[rclone].max_concurrent_moves=%d",
                        self.general.max_concurrent_moves,
                        self.rclone.max_concurrent_moves,
                    )
            except Exception:
                pass
        except Exception:
            pass
        return self

    def is_episode(self, name: str) -> bool:
        return bool(self.classifier._episode_re.search(name))

    @model_validator(mode="after")
    def _cross_validate_storage(self) -> AppConfig:
        """Reject storage layouts that silently corrupt data.

        SSD dirs nested inside (or containing) a fuse mount misclassify
        every torrent (on_fuse detection, move sources and wipe guards all
        assume disjoint trees) — advisory warnings were not enough. Paths
        resolve (symlinks, `..`, case) before comparison so an SSD
        symlinked into a fuse mount cannot slip past string matching.
        """

        def _norm(p: object) -> str:
            try:
                import os as _os

                resolved = str(Path(str(p or "")).resolve())
                if _os.name == "nt":
                    return resolved.lower()
                return resolved
            except Exception:
                pass
            try:
                return str(p or "").rstrip("/\\").replace("\\", "/")
            except Exception:
                return ""

        def _nested(inner: str, outer: str) -> bool:
            a, b = inner.replace("\\", "/"), outer.replace("\\", "/")
            return bool(a and b and (a == b or a.startswith(b + "/")))

        try:
            ssd_dirs = [_norm(self.ssd.path), _norm(self.dest.save_path)]
        except Exception:
            return self
        local_dirs = []
        for raw in (self.pipeline.local_path, self.pipeline.local_unsorted):
            if raw is not None:
                local_dirs.append(_norm(raw))
        for dest in local_dirs:
            for s in ssd_dirs:
                if _nested(dest, s) or _nested(s, dest):
                    raise ValueError(
                        f"storage overlap: local_move destination {dest!r} overlaps "
                        f"SSD path {s!r} — use disjoint trees"
                    )
        if self.rclone.fuse is None:
            return self
        try:
            fuse = [_norm(self.rclone.fuse.mount), _norm(self.rclone.fuse.mount_unsorted)]
        except Exception:
            return self
        for s in ssd_dirs:
            for fm in fuse:
                if _nested(s, fm) or _nested(fm, s):
                    raise ValueError(
                        f"storage overlap: SSD path {s!r} overlaps fuse mount "
                        f"{fm!r} — use disjoint paths ([ssd].path/[dest].save_path "
                        "must not equal or nest inside [rclone.fuse] mounts)"
                    )
        return self


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
                    key,
                    f" in [{prefix}]" if prefix else "",
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

        origin = _typing.get_origin(annotation)
        args = [a for a in (_typing.get_args(annotation) or ()) if isinstance(a, type)]
        if origin in (_typing.Union, getattr(_types, "UnionType", _typing.Union)):
            for a in args:
                if isinstance(a, type) and issubclass(a, BaseModel):
                    _warn_unknown_keys(a, val, prefix)
                    return
            return
        # Lists/tuples of models (e.g. [[prowlarr.download_indexers]]):
        # descend into each element instead of going blind.
        if origin in (list, tuple, set, frozenset):
            if not isinstance(val, (list, tuple)):
                return
            for i, item in enumerate(val):
                for a in args:
                    try:
                        if isinstance(a, type) and issubclass(a, BaseModel):
                            _warn_unknown_keys(a, item, f"{prefix}[{i}]")
                        else:
                            _descend_unknown(a, item, f"{prefix}[{i}]")
                    except Exception:
                        continue
            return
        if origin is dict:
            return
        if not isinstance(val, dict):
            return
        ann = annotation
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            _warn_unknown_keys(ann, val, prefix)
    except Exception:
        pass
