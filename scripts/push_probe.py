#!/usr/bin/env python3
"""VPS1 rclone push probe — a ladder test through the app's OWN SSH dial.

Why this exists: the push lane launched 28 copies and none of them ever
landed a file, while the app reported "push started". Every short call now
goes through `SFTPExporter.run_shell_dedicated` and every launch through
`launch_detached`, so this script exercises the SAME code paths rather than
a hand-written approximation of them. Each step prints raw remote output;
the last steps settle the question the app log cannot answer on its own:
whether a detached job on VPS1 actually survives the SSH channel closing.

Run it on the machine that runs the app (VPS2), with the production config:

    python3 scripts/push_probe.py --config debug-config.toml
    python3 scripts/push_probe.py --config debug-config.toml --copy-test

`--copy-test` is opt-in and writes to the remote on purpose: it creates a
2 MiB scratch file on VPS1, pushes it with the real launch path to
`<remote>:racing-sync-probe-<token>/`, verifies it landed, then deletes
both copies. Nothing else in this script writes anywhere.

No credential is ever printed: the SSH section is reported as host/port/user
plus whether a key or password is configured.
"""

from __future__ import annotations

import argparse
import asyncio
import shlex
import sys
import time
from pathlib import Path
from secrets import token_hex

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from racing_sync import vps1_push as vp  # noqa: E402
from racing_sync.config import AppConfig  # noqa: E402
from racing_sync.runtime import Runtime  # noqa: E402
from racing_sync.sftp_source import SFTPExporter  # noqa: E402

_RESULTS: list[tuple[str, str, str]] = []


def report(name: str, status: str, detail: str = "") -> bool:
    """Record and print one step. Status: PASS / FAIL / WARN / INFO."""
    _RESULTS.append((name, status, detail))
    print(f"[{status:>4}] {name}" + (f"\n       {detail}" if detail else ""))
    return status == "PASS"


def show(raw: tuple[int | None, str, str]) -> str:
    """Render (rc, stdout, stderr) compactly for a FAIL line."""
    rc, out, err = raw
    bits = [f"rc={rc}"]
    if (out or "").strip():
        bits.append(f"out={out.strip()[:300]!r}")
    if (err or "").strip():
        bits.append(f"err={err.strip()[:300]!r}")
    return " ".join(bits)


def sh(ssh, command: str, timeout: float) -> tuple[int | None, str, str]:
    """One short remote command through the app's dedicated dial."""
    return ssh.run_shell_dedicated(command, timeout)


def probe_transport(ssh, timeout: float) -> None:
    rc, out, err = sh(ssh, "uname -a && whoami && date -u", timeout)
    if rc == 0:
        report("ssh dial (dedicated transport)", "PASS", (out or "").strip().replace("\n", " | "))
    else:
        report("ssh dial (dedicated transport)", "FAIL", show((rc, out, err)))


def probe_binaries(ssh, timeout: float, rclone_bin: str) -> dict[str, str | None]:
    found: dict[str, str | None] = {}
    for label, cmd in (
        ("rclone", f"command -v {shlex.quote(rclone_bin)}"),
        ("setsid", "command -v setsid"),
        ("nohup", "command -v nohup"),
        ("pgrep", "command -v pgrep"),
    ):
        rc, out, err = sh(ssh, cmd, timeout)
        path = (out or "").strip().splitlines()
        found[label] = path[0] if rc == 0 and path else None
        report(
            f"VPS1 has {label}",
            "PASS" if found[label] else ("WARN" if label == "setsid" else "FAIL"),
            found[label] or show((rc, out, err)),
        )

    rc, out, err = sh(ssh, f"{shlex.quote(rclone_bin)} version 2>&1 | head -n 3", timeout)
    if rc != 0 or not (out or "").strip():
        # Older rclone spells it --version; report whichever answers.
        rc, out, err = sh(ssh, f"{shlex.quote(rclone_bin)} --version 2>&1 | head -n 3", timeout)
    report(
        "rclone version on VPS1",
        "PASS" if (out or "").strip() else "FAIL",
        (out or "").strip().replace("\n", " / ") or show((rc, out, err)),
    )
    return found


def probe_remotes(ssh, timeout: float, rclone_bin: str, want: str, cfg) -> None:
    rc, out, err = sh(ssh, f"{shlex.quote(rclone_bin)} listremotes 2>&1", timeout)
    listed = [ln.strip().rstrip(":") for ln in (out or "").splitlines() if ln.strip()]
    if rc != 0 or not listed:
        report(
            "rclone listremotes on VPS1",
            "FAIL",
            show((rc, out, err)) or "no remotes configured on VPS1",
        )
        return
    report(
        "rclone listremotes on VPS1",
        "PASS" if want in listed else "FAIL",
        f"found={listed} — app wants {want!r}" + ("" if want in listed else " (MISMATCH)"),
    )

    rt = Runtime(cfg)
    listing = (
        ("rclone lsd remote:", f"{shlex.quote(rclone_bin)} lsd {shlex.quote(f'{want}:')}"),
        (
            "lsf seed remote (sorted)",
            f"{shlex.quote(rclone_bin)} lsf {shlex.quote(rt.transfer.remote_default)} --max-depth 1",
        ),
        (
            "lsf seed remote (unsorted)",
            f"{shlex.quote(rclone_bin)} lsf {shlex.quote(rt.transfer.remote_unsorted)} --max-depth 1",
        ),
    )
    for label, cmd in listing:
        rc, out, err = sh(ssh, cmd, timeout)
        head = (out or "").strip().replace("\n", " | ")[:220]
        report(label, "PASS" if rc == 0 else "WARN", head or show((rc, out, err)))


def probe_detach(ssh, timeout: float, binaries: dict, secs: int) -> None:
    """The decisive test: does a detached job survive the channel closing?"""
    detacher = "setsid" if binaries.get("setsid") else ("nohup" if binaries.get("nohup") else None)
    if detacher is None:
        report("detached-job test", "FAIL", "neither setsid nor nohup on VPS1")
        return

    token = token_hex(4)
    log = f"/tmp/racing-sync-probe-{token}.log"
    inner = f"echo STARTED > {log}; sleep {secs}; echo DONE >> {log}"
    command = f"{detacher} sh -c {shlex.quote(inner)} </dev/null >/dev/null 2>&1 &"
    if not ssh.launch_detached(command):
        report("detached-job launch", "FAIL", "launch_detached() returned False")
        return
    report("detached-job launch", "PASS", f"wrapper={detacher} cmd={command[:120]}")

    time.sleep(2)
    rc, out, _ = sh(ssh, f"cat {shlex.quote(log)} 2>&1", timeout)
    started = "STARTED" in (out or "")
    report("job ran at all (2s after launch)", "PASS" if started else "FAIL", (out or "").strip())

    rc, out, _ = sh(ssh, "pgrep -af sleep 2>&1 | head -n 5", timeout)
    report("pgrep sees the job", "INFO", (out or "").strip()[:200] or show((rc, out, "")))

    time.sleep(secs + 2)
    rc, out, _ = sh(ssh, f"cat {shlex.quote(log)} 2>&1", timeout)
    body = (out or "").strip()
    survived = "DONE" in body
    report(
        "job SURVIVED the channel close",
        "PASS" if survived else "FAIL",
        body
        + (
            ""
            if survived
            else " — STARTED but never DONE: VPS1 kills detached children when the "
            "session ends, so the push must be reparented differently (systemd-run/at)"
        ),
    )
    sh(ssh, f"rm -f {shlex.quote(log)}", timeout)


async def probe_app_path(ssh, cfg, cmd_timeout: float, copy_test: bool = False) -> None:
    """Run the app's real preflight and show the exact command it would exec."""
    section = cfg.source.rclone_push
    src_dir = "/tmp/racing-sync-probe-src"
    sh(ssh, f"mkdir -p {shlex.quote(src_dir)}", cmd_timeout)
    rt_view = Runtime(cfg)
    target = str(rt_view.transfer.remote_unsorted or "")
    req = vp.PushRequest(
        infohash="a" * 40,
        group_key="probe",
        content_name="probe",
        src_dir=src_dir,
        target_remote=target,
        files=(vp.PushFile("probe.bin", 2048),),
    )
    check = await vp.preflight(
        req,
        ssh=ssh,
        cfg=cfg,
        remote_name=str(getattr(section, "remote_name", "") or ""),
        rclone_bin=str(getattr(section, "rclone_bin", "rclone") or "rclone"),
        timeout_s=cmd_timeout,
    )
    report(
        "app preflight()",
        "PASS" if check.ok else "FAIL",
        f"ok={check.ok} reason={check.reason!r} detacher={getattr(check, 'detacher', None)!r} "
        f"error_on_no_transfer={getattr(check, 'error_on_no_transfer', None)!r}",
    )
    cmd = vp.build_copy_command(
        req,
        rclone_bin=str(getattr(section, "rclone_bin", "rclone") or "rclone"),
        transfers=int(getattr(section, "transfers", 2) or 2),
        bwlimit=str(getattr(section, "bwlimit", "") or ""),
        list_path="/tmp/racing-sync-probe.lst",
        log_path="/tmp/racing-sync-probe.log",
        detacher=str(getattr(check, "detacher", "setsid") or "setsid"),
        error_on_no_transfer=bool(getattr(check, "error_on_no_transfer", True)),
    )
    report(
        "exact command the app would exec",
        "INFO",
        cmd,
    )
    print("\n  Paste that line into a shell ON VPS1 (without the trailing &) to see")
    print("  rclone's own error, if it has one.\n")

    if not copy_test:
        print("  Re-run with --copy-test to push a real 2 MiB file through the app's own")
        print("  start_copy() and settle the whole path end to end.\n")
        return

    token = token_hex(4)
    bin_name = f"probe-{token}.bin"
    scratch = f"/tmp/racing-sync-probe-{token}"
    rc, out, err = sh(
        ssh,
        f"mkdir -p {shlex.quote(scratch)} && head -c 2097152 /dev/urandom > "
        f"{shlex.quote(f'{scratch}/{bin_name}')} && ls -l {shlex.quote(scratch)}",
        cmd_timeout,
    )
    if rc != 0:
        report("copy-test setup on VPS1", "FAIL", show((rc, out, err)))
        return
    remote = vp.derive_remote_name(target, str(getattr(section, "remote_name", "") or ""))
    dst = f"{remote}:racing-sync-probe-{token}/"
    real = vp.PushRequest(
        infohash="b" * 40,
        group_key="probe",
        content_name="probe",
        src_dir=scratch,
        target_remote=dst,
        files=(vp.PushFile(bin_name, 2097152),),
    )
    launched = await vp.start_copy(
        real,
        ssh=ssh,
        rclone_bin=str(getattr(section, "rclone_bin", "rclone") or "rclone"),
        transfers=1,
        bwlimit="",
        detacher=str(getattr(check, "detacher", "setsid") or "setsid"),
        error_on_no_transfer=bool(getattr(check, "error_on_no_transfer", True)),
        proof_delay_s=3,
    )
    report(
        "app start_copy() of a real 2 MiB file",
        "PASS" if launched.ok else "FAIL",
        f"ok={launched.ok} reason={launched.reason!r} log={launched.log_path!r}",
    )
    if launched.ok:
        rc, out, err = sh(ssh, f"tail -n 20 {shlex.quote(launched.log_path)} 2>&1", cmd_timeout)
        report("job log on VPS1 (first 20 lines)", "INFO", (out or "").strip()[:400])
        landed = (0, 0)
        for _ in range(20):
            await asyncio.sleep(3)
            landed = await vp.remote_progress(cfg, dst, real.files, timeout_s=cmd_timeout)
            if landed[0] >= landed[1] and landed[1] > 0:
                break
        report(
            "file visible on the seed remote",
            "PASS" if landed[1] and landed[0] >= landed[1] else "FAIL",
            f"{landed[0]}/{landed[1]} at {dst}",
        )
        rc, out, err = sh(
            ssh,
            f"{shlex.quote(str(getattr(section, 'rclone_bin', 'rclone') or 'rclone'))} "
            f"delete --rmdirs {shlex.quote(dst)} 2>&1",
            cmd_timeout,
        )
        report("remote probe folder removed", "PASS" if rc == 0 else "WARN", show((rc, out, err)))
    sh(ssh, f"rm -rf {shlex.quote(scratch)}", cmd_timeout)


def summary() -> int:
    print("=" * 72)
    bad = [n for n, s, _ in _RESULTS if s == "FAIL"]
    for name, status, detail in _RESULTS:
        if status in ("FAIL", "WARN"):
            print(f"  {status}  {name}: {detail[:120]}")
    if bad:
        print(f"\n{len(bad)} FAILED step(s). The push lane cannot work until these do.")
        return 1
    print("\nAll steps passed: the app can drive rclone on VPS1.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe the VPS1 rclone push path.")
    ap.add_argument("--config", default="debug-config.toml", help="app config to use")
    ap.add_argument("--timeout", type=float, default=30.0, help="per-command timeout (s)")
    ap.add_argument("--detach-secs", type=int, default=12, help="how long the job must survive")
    ap.add_argument(
        "--copy-test",
        action="store_true",
        help="push a real 2 MiB scratch file through start_copy() to the remote, verify it "
        "landed, then delete it (the only step that writes to the remote)",
    )
    args = ap.parse_args()

    path = Path(args.config)
    if not path.exists():
        print(f"config not found: {path}")
        return 2
    cfg = AppConfig.from_toml(path)
    sftp_cfg = getattr(cfg.source, "deluge_sftp", None)
    if sftp_cfg is None or not getattr(sftp_cfg, "enabled", False):
        print("[source.deluge_sftp] is missing or disabled — the push has no SSH dial.")
        return 2
    section = getattr(cfg.source, "rclone_push", None)
    print(
        f"dial: {getattr(sftp_cfg, 'ssh_host', '')}:{getattr(sftp_cfg, 'ssh_port', '')} "
        f"user={getattr(sftp_cfg, 'ssh_user', '')} "
        f"key={'set' if getattr(sftp_cfg, 'ssh_key_path', None) else 'none'} "
        f"password={'set' if getattr(sftp_cfg, 'ssh_password', None) else 'none'}\n"
        f"push: enabled={getattr(section, 'enabled', None)} "
        f"auto={getattr(section, 'auto_on_prowlarr_timeout', None)} "
        f"pressure={getattr(section, 'push_on_janitor_pressure', None)} "
        f"remote_name={getattr(section, 'remote_name', '')!r} "
        f"rclone_bin={getattr(section, 'rclone_bin', '')!r}\n"
    )

    ssh = SFTPExporter(sftp_cfg)
    want = vp.derive_remote_name(
        Runtime(cfg).transfer.remote_unsorted or "",
        str(getattr(section, "remote_name", "") or ""),
    )
    try:
        probe_transport(ssh, args.timeout)
        binaries = probe_binaries(ssh, args.timeout, str(getattr(section, "rclone_bin", "rclone")))
        probe_remotes(ssh, args.timeout, str(getattr(section, "rclone_bin", "rclone")), want, cfg)
        probe_detach(ssh, args.timeout, binaries, args.detach_secs)
        asyncio.run(probe_app_path(ssh, cfg, args.timeout, args.copy_test))
    except Exception as e:  # noqa: BLE001
        report("probe aborted", "FAIL", f"{type(e).__name__}: {e}")
    finally:
        try:
            ssh.close()
        except Exception:
            pass
    return summary()


if __name__ == "__main__":
    sys.exit(main())
