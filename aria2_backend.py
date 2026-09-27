#!/usr/bin/env python3
"""aria2 backend for LDL (L30 Download Manager).

Wraps the `aria2c` binary (https://aria2.github.io/) as the download engine
while keeping LDL's Rich UI / job model on top.

Why aria2:
  - Multi-connection segmented HTTP(S)/FTP/SFTP out of the box (-x/-s/-k)
  - Native BitTorrent (incl. magnet), Metalink support (native engine can't)
  - Automatic resume with -c, retry, timeout handling in C++

This module mirrors the `download_with_ytdlp` interface in downloader.py so
main() can route jobs through it with minimal changes:
    download_with_aria2(url, out_dir, limiter, paused, stop_event,
                        job, throughput, connections=8, timeout=30,
                        retries=5, extra_args=None) -> bool
"""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import List, Optional

ARIA2C_BIN = shutil.which("aria2c")


def is_available() -> bool:
    return ARIA2C_BIN is not None


def need_aria2() -> str:
    return "aria2c not found. Install with: brew install aria2  (or apt install aria2)"


_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMGT]?i?B?)?\s*$", re.I)
# e.g. "[#2089b0 10MiB/100MiB(10%) CN:8 DL:5.0MiB ETA:18s]"
_PROGRESS_RE = re.compile(
    r"\[#\w+\s+(\S+)/(\S+)\((\d+(?:\.\d+)?)%\)[^\]]*?CN:(\d+)(?:[^\]]*?DL:(\S+))?",
    re.I,
)
_COMPLETE_RE = re.compile(r"Download complete:\s*(.+?)\s*$", re.I)


def parse_aria2_size(s: str) -> int:
    """Parse aria2 human sizes like '10MiB', '5.0MiB', '1024', '1.5G' -> bytes."""
    if not s or s in ("0", "n/a", "N/A", "--"):
        return 0
    s = s.strip().rstrip("/s").rstrip("s") if s.endswith("s") else s.strip()
    # strip trailing 's' from speeds like '5.0MiBs' / '10KB/s'
    s = re.sub(r"/s$", "", s, flags=re.I).rstrip("s") if s.lower().endswith("ibs") else s
    m = _SIZE_RE.match(s.strip())
    if not m:
        return 0
    val = float(m.group(1))
    unit = (m.group(2) or "").upper().replace("I", "").replace("B", "")
    mult = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    return int(val * mult.get(unit, 1))


def format_limit_for_aria2(bps: float) -> str:
    """Convert bytes/sec -> aria2 --max-overall-download-limit value (e.g. '5M')."""
    if bps <= 0:
        return "0"
    if bps >= 1024 ** 3:
        v = bps / 1024 ** 3
        return f"{v:.1f}G".rstrip("0").rstrip(".") + "" if False else f"{int(v)}G" if v == int(v) else f"{v:.1f}G"
    if bps >= 1024 ** 2:
        v = bps / 1024 ** 2
        return f"{int(v)}M" if v == int(v) else f"{v:.1f}M"
    if bps >= 1024:
        v = bps / 1024
        return f"{int(v)}K" if v == int(v) else f"{v:.1f}K"
    return str(int(bps))


def is_torrent_like(url: str) -> bool:
    u = url.strip().lower()
    return (
        u.startswith("magnet:")
        or u.endswith(".torrent")
        or u.endswith(".meta4")
        or u.endswith(".metalink")
    )


def build_cmd(url: str, out_dir: Path, connections: int = 8,
              speed_limit_bps: float = 0, timeout: float = 30,
              retries: int = 5, user_agent: str = "",
              extra_args: Optional[List[str]] = None) -> List[str]:
    if not ARIA2C_BIN:
        raise FileNotFoundError(need_aria2())
    connections = max(1, min(32, connections))
    cmd = [
        ARIA2C_BIN,
        "--continue=true",
        "--allow-overwrite=false",
        "--auto-file-renaming=false",
        f"--dir={str(out_dir)}",
        f"--max-connection-per-server={connections}",  # -x
        f"--split={connections}",                       # -s
        "--min-split-size=1M",                           # -k (finer than 20M default)
        "--max-concurrent-downloads=1",                  # LDL batches sequentially
        f"--max-tries={max(0, retries)}",
        f"--timeout={int(timeout)}",
        f"--connect-timeout={min(int(timeout), 60)}",
        "--retry-wait=3",
        "--check-certificate=true",
        "--remote-time=true",
        "--summary-interval=1",
        "--console-log-level=warn",
        "--human-readable=true",
        "--show-console-readout=true",
        "--follow-torrent=true",
        "--follow-metalink=true",
        "--seed-time=0",  # don't seed torrents by default (leech mode)
    ]
    if speed_limit_bps and speed_limit_bps > 0:
        cmd.append(f"--max-overall-download-limit={format_limit_for_aria2(speed_limit_bps)}")
    else:
        cmd.append("--max-overall-download-limit=0")
    if user_agent:
        cmd.append(f"--user-agent={user_agent}")
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(url)
    return cmd


def download_with_aria2(url: str, out_dir: Path, limiter, paused: threading.Event,
                        stop_event: threading.Event, job, throughput,
                        connections: int = 8, timeout: float = 30,
                        retries: int = 5,
                        extra_args: Optional[List[str]] = None) -> bool:
    """Run one URL through aria2c, feeding progress into LDL's FileJob."""
    job.start_time = time.monotonic()
    job.status = "downloading"
    job.detail = "aria2 engine"
    job.connections_used = connections
    out_dir.mkdir(parents=True, exist_ok=True)

    if not is_available():
        job.status = "failed"
        job.detail = need_aria2()
        return False

    # snapshot dir so we can detect the finished filename afterwards
    before = set(out_dir.iterdir()) if out_dir.exists() else set()
    rate = limiter.rate if hasattr(limiter, "rate") else 0
    try:
        user_agent = ""
        try:
            from downloader import USER_AGENT as _UA
            user_agent = _UA
        except Exception:
            user_agent = "LDownloader/1.0.0 (aria2)"
        cmd = build_cmd(url, out_dir, connections, rate, timeout, retries,
                        user_agent, extra_args)
    except Exception as exc:
        job.status = "failed"
        job.detail = str(exc)[:120]
        return False

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1)
    except Exception as exc:
        job.status = "failed"
        job.detail = f"aria2 spawn failed: {exc}"[:120]
        return False

    completed_path: Optional[str] = None
    last_downloaded = 0

    def feed_line(line: str) -> None:
        nonlocal last_downloaded, completed_path
        # aria2 redraws progress with '\r' on one console line, so a single
        # read chunk may contain several updates; process ALL matches and
        # keep the last one as current state (absolute bytes -> delta stays
        # correct even when intermediate updates coalesce).
        if not line:
            return
        mc = _COMPLETE_RE.search(line)
        if mc:
            completed_path = mc.group(1).strip()
            job.detail = f"done: {Path(completed_path).name}"[:90]
            return
        matches = _PROGRESS_RE.findall(line)
        if matches:
            try:
                dls, tots, pcts, cns, spd = matches[-1]
                total = parse_aria2_size(tots)
                downloaded = parse_aria2_size(dls)
                if total > 0:
                    job.total = total
                # aria2 reports cumulative per-file bytes; convert to delta
                if downloaded >= last_downloaded:
                    delta = downloaded - last_downloaded
                else:  # restart / retry
                    delta = downloaded
                last_downloaded = downloaded
                job.downloaded = downloaded
                if delta > 0:
                    throughput.add(delta)
                    try:
                        job.tracker.add(delta)
                    except Exception:
                        pass
                try:
                    job.connections_used = int(cns)
                except Exception:
                    pass
                job.detail = line.strip().split("\r")[-1][-90:]
            except Exception:
                job.detail = line.strip().split("\r")[-1][-90:]
            return
        # keep last meaningful status line (errors, results) for UI/summary
        for chunk in line.split("\r"):
            chunk = chunk.strip()
            if not chunk:
                continue
            low = chunk.lower()
            if any(k in low for k in ("error", "failed", "exception", "retry", "download results", "status", "gid")):
                job.detail = chunk[-90:]

    def pump():
        assert proc.stdout is not None
        for line in proc.stdout:
            feed_line(line)
            if stop_event.is_set():
                break

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    try:
        while proc.poll() is None:
            if stop_event.is_set():
                try:
                    proc.terminate()
                except Exception:
                    pass
                break
            if paused.is_set():
                job.status = "paused"
                try:
                    if os.name == "posix":
                        proc.send_signal(signal.SIGSTOP)
                        while paused.is_set() and proc.poll() is None and not stop_event.is_set():
                            time.sleep(0.1)
                        if proc.poll() is None:
                            proc.send_signal(signal.SIGCONT)
                    else:
                        time.sleep(0.2)
                except Exception:
                    time.sleep(0.2)
            else:
                if job.status != "downloading":
                    job.status = "downloading"
                time.sleep(0.2)
    except KeyboardInterrupt:
        stop_event.set()
        try:
            proc.terminate()
        except Exception:
            pass
    rc = proc.wait()
    job.end_time = time.monotonic()

    # resolve final filename -> point job at the real file for the summary table
    final: Optional[Path] = None
    if completed_path:
        p = Path(completed_path)
        if p.exists():
            final = p
    if final is None:
        try:
            after = set(out_dir.iterdir())
            new_files = [p for p in (after - before) if p.is_file() and not p.name.endswith((".aria2", ".torrent"))]
            if new_files:
                # pick largest (the download) if several
                new_files.sort(key=lambda p: p.stat().st_size, reverse=True)
                final = new_files[0]
        except Exception:
            pass
    if final is not None:
        try:
            job.dest = final
            if not job.total:
                job.total = final.stat().st_size
            # ensure downloaded reflects reality at completion; reconcile
            # session throughput for coalesced \r updates missed mid-run
            if rc == 0:
                real_size = final.stat().st_size
                if real_size > last_downloaded:
                    missing = real_size - last_downloaded
                    throughput.add(missing)
                    try:
                        job.tracker.add(missing)
                    except Exception:
                        pass
                    last_downloaded = real_size
                job.downloaded = max(job.downloaded, real_size)
        except Exception:
            pass

    if rc == 0 and not stop_event.is_set():
        job.status = "done"
        job.detail = ""
        try:
            elapsed = max(0.001, job.end_time - job.start_time) if job.start_time else 0.001
            job.avg_speed = job.downloaded / elapsed
        except Exception:
            pass
        return True
    if stop_event.is_set():
        job.status = "stopped"
    else:
        job.status = "failed"
        if not job.detail or job.detail == "aria2 engine":
            job.detail = f"aria2 exit {rc}"
    return False
