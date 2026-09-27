#!/usr/bin/env python3
"""
High-performance segmented file downloader.

Features:
  - Multi-connection segmented HTTP downloads (Range requests) for maximum speed
  - Single-stream fallback for servers without Range support / unknown size
  - Global speed limit (token bucket), adjustable at runtime
  - Pause / resume with keyboard controls, persistent resume across restarts
  - Exponential-backoff retry, per-segment retry and re-split
  - Batch mode: multiple URLs, one per line in a text file
  - Optional yt-dlp backend for video / site URLs
  - Detailed Rich terminal UI (no emoji)

Usage:
  python downloader.py <url> [url ...] -o ~/Downloads
  python downloader.py -f urls.txt -o ~/Downloads --connections 16 --speed-limit 5M
  python downloader.py "https://youtube.com/watch?v=..." --yt-dlp

Controls while downloading (when stdin is a TTY):
  P or Space : pause / resume toggle
  R          : resume
  + / -      : increase / decrease speed limit
  U          : unlimited speed
  Q or Ctrl+C: graceful quit (state saved, resume on next run)
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import random
import re
import select
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

try:
    import httpx
except ImportError:
    print("Missing dependency: httpx. Install with: pip install httpx rich yt-dlp", file=sys.stderr)
    sys.exit(2)

try:
    from rich.console import Console, Group
    from rich.columns import Columns
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("Missing dependency: rich. Install with: pip install rich", file=sys.stderr)
    sys.exit(2)


APP_NAME = "L Downloader"
APP_VERSION = "1.0.0"

# Optional startup banner gallery (LDL/banners.py). Resolved via the real
# script directory so it works both as `python py/LDL/downloader.py` and
# via the symlinked `dl` command. Any failure falls back to the small LOGO.
try:
    _HERE = Path(__file__).resolve().parent
    if str(_HERE) not in sys.path:
        sys.path.insert(0, str(_HERE))
    from banners import BANNERS as _BANNERS
    from banners import get_banner as _get_banner
    from banners import pick_banner as _pick_banner
    from banners import scale_art as _scale_art
    from banners import SPLASH_DURATION as _SPLASH_DURATION
    from banners import SPLASH_FPS as _SPLASH_FPS
    from banners import grid_size as _grid_size
    from banners import iter_cells as _iter_cells
    _BANNERS_AVAILABLE = bool(_BANNERS)
except Exception:
    _BANNERS = []
    _BANNERS_AVAILABLE = False
    _get_banner = None  # type: ignore[assignment]
    _pick_banner = None  # type: ignore[assignment]
    _scale_art = None  # type: ignore[assignment]
    _SPLASH_DURATION = 4.0
    _SPLASH_FPS = 25
    _grid_size = None  # type: ignore[assignment]
    _iter_cells = None  # type: ignore[assignment]


def _tty_key_pressed(fd: int) -> bool:
    """Non-blocking check for a keypress on fd; consumes the key."""
    if fd < 0:
        return False
    try:
        r, _, _ = select.select([fd], [], [], 0)
        if not r:
            return False
        os.read(fd, 1)
        return True
    except Exception:
        return False


def animate_splash(console: Console, art: str,
                   duration: float = _SPLASH_DURATION,
                   fps: int = _SPLASH_FPS) -> None:
    """Dissolve-in reveal of banner art with a glow on fresh cells.

    Ink cells appear in shuffled order over `duration` seconds; the batch
    revealed in the current frame glows bright while older ink rests in
    bold cyan. Any keypress skips straight to the full art. Never raises:
    any failure falls back to an instant print.
    """
    try:
        rows, cols = _grid_size(art)
        if rows <= 0 or cols <= 0:
            return
        cells = list(_iter_cells(art))
        if not cells:
            console.print(Text(art, style="bold cyan", no_wrap=True))
            return
        random.shuffle(cells)
        frames = max(1, int(round(duration * fps)))
        per_frame = max(1, (len(cells) + frames - 1) // frames)

        # Keypress-skip reads /dev/tty so piped stdin is unaffected.
        tty_fd = -1
        old_attrs = None
        try:
            import termios
            import tty
            tty_fd = os.open("/dev/tty", os.O_RDONLY)
            old_attrs = termios.tcgetattr(tty_fd)
            tty.setcbreak(tty_fd)
        except Exception:
            tty_fd = -1
            old_attrs = None

        base_lines = [l.rstrip() for l in art.split("\n")]

        def render(revealed, fresh) -> Text:
            grid = [list(l.ljust(cols)) for l in base_lines[:rows]]
            for (r, c, ch) in revealed:
                grid[r][c] = ch
            out = Text(no_wrap=True)
            for r in range(rows):
                row_str = "".join(grid[r]).rstrip(" ")
                seg = Text(row_str, style="bold cyan")
                for (fr, fc) in fresh:
                    if fr == r and fc < len(row_str):
                        seg.stylize("bold bright_white", fc, fc + 1)
                out.append(seg)
                if r < rows - 1:
                    out.append("\n")
            return out

        try:
            revealed: list = []
            idx = 0
            frame_dt = 1.0 / max(1, fps)
            start = time.monotonic()
            frame_no = 0
            with Live(render([], set()), console=console,
                      refresh_per_second=fps, transient=True) as live:
                while idx < len(cells):
                    if _tty_key_pressed(tty_fd):
                        break
                    nxt = min(len(cells), idx + per_frame)
                    fresh = set((r, c) for r, c, _ in cells[idx:nxt])
                    revealed.extend(cells[idx:nxt])
                    idx = nxt
                    live.update(render(revealed, fresh))
                    target = start + (frame_no + 1) * frame_dt
                    delay = target - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    frame_no += 1
        finally:
            if tty_fd >= 0:
                try:
                    import termios
                    if old_attrs is not None:
                        termios.tcsetattr(tty_fd, termios.TCSADRAIN, old_attrs)
                except Exception:
                    pass
                try:
                    os.close(tty_fd)
                except Exception:
                    pass
        # Final state: full art, identical to the static print.
        console.print(Text(art, style="bold cyan", no_wrap=True))
    except Exception:
        try:
            console.print(Text(art, style="bold cyan", no_wrap=True))
        except Exception:
            pass


def resolve_splash(console: Console, args):
    """Choose the run's banner once. Returns (art_or_None, rc).

    rc=2 on invalid --banner. Never raises.
    """
    if getattr(args, "no_banner", False) or not _BANNERS_AVAILABLE:
        return None, 0
    try:
        forced = getattr(args, "banner", None)
        if forced is not None:
            try:
                return _get_banner(forced), 0
            except ValueError as exc:
                print(str(exc), file=sys.stderr)
                return None, 2
        width = console.width or 80
        return _pick_banner(max(20, width - 2)), 0
    except Exception:
        return None, 0


def show_splash(console: Console, art) -> None:
    """Display a resolved banner: animated on TTY, instant when piped."""
    if art is None:
        return
    try:
        if console.is_terminal:
            animate_splash(console, art)
        else:
            # Piped output: instant print, no control sequences.
            console.print(Text(art, style="bold cyan", no_wrap=True))
    except Exception:
        pass


def print_splash(console: Console, args) -> int:
    """Print one random (or forced) banner. Returns 0 ok, 2 on bad --banner.

    Runs once per process before the live UI; never raises.
    """
    art, rc = resolve_splash(console, args)
    if rc:
        return rc
    show_splash(console, art)
    return 0
LOGO = (
    "█     ████  █\n"
    "█░    █░░░█ █░\n"
    "█░░   █░░░█░█░░\n"
    "█░░   █░░ █░█░░\n"
    "█████ ████ ░█████\n"
    " ░░░░░ ░░░░ ░░░░░░\n"
    "  ░░░░░ ░░░░  ░░░░░"
)
_LOGO_W = max(len(l) for l in LOGO.split("\n"))

# Header mini art: right-side banner scaled to a fixed row budget so the
# header never changes height. Scaled Text cached per (art, width).
MINI_ROWS = 8
MINI_MIN_TERM = 100
_MINI_CACHE = {}


def _mini_text(art, term_width):
    """Scaled right-side header art, or None when it should be hidden.

    Hidden when banners are unavailable, the terminal is narrow, or the
    slot is too small. Returned Text is always exactly MINI_ROWS lines.
    """
    if not art or not _BANNERS_AVAILABLE or _scale_art is None:
        return None
    if term_width < MINI_MIN_TERM:
        return None
    slot = term_width - _LOGO_W - 3 - 4 - 2
    if slot < 16:
        return None
    key = (hash(art), slot, MINI_ROWS)
    hit = _MINI_CACHE.get(key)
    if hit is not None:
        return hit
    try:
        lines = _scale_art(art, slot, MINI_ROWS)
    except Exception:
        return None
    # Right-align: left-pad so the block hugs the panel's right edge.
    content = [((" " * (slot - len(l))) + l) if len(l) < slot else l[:slot]
               for l in lines]
    txt = Text("\n".join(content), style="cyan", no_wrap=True)
    if len(_MINI_CACHE) > 24:
        _MINI_CACHE.pop(next(iter(_MINI_CACHE)))
    _MINI_CACHE[key] = txt
    return txt
CHUNK_SIZE = 256 * 1024
MIN_SEGMENTED_SIZE = 8 * 1024 * 1024
STATE_SUFFIX = ".part.json"
PART_SUFFIX = ".part"
DEFAULT_CONNECTIONS = 8
MAX_CONNECTIONS = 32
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 30.0
STATE_SAVE_INTERVAL = 2.0

USER_AGENT = f"LDownloader/{APP_VERSION} (httpx; segmented)"

VIDEO_HINTS = ("youtube.com", "youtu.be", "vimeo.com", "tiktok.com",
               "instagram.com", "twitch.tv", "dailymotion.com", "x.com", "twitter.com")


# ---------------------------------------------------------------------------
# Formatting / parsing helpers
# ---------------------------------------------------------------------------

def parse_rate(s: str) -> float:
    """Parse '0', '500K', '5M', '1.5G', '100KB/s' -> bytes/sec. 0 = unlimited."""
    if s is None:
        return 0.0
    t = str(s).strip().upper().replace("/S", "").replace("/SEC", "").strip()
    if t in ("0", "UNLIMITED", "NONE", ""):
        return 0.0
    m = re.fullmatch(r"([0-9]*\.?[0-9]+)\s*([KMGT]?I?B?)?", t)
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid speed limit: {s!r} (examples: 0, 500K, 5M, 20M)")
    val = float(m.group(1))
    unit = (m.group(2) or "").replace("I", "").replace("B", "")
    mult = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    return val * mult.get(unit, 1)


def format_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    if n <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = min(int(math.log(n, 1024)), len(units) - 1) if n >= 1 else 0
    v = n / (1024 ** i)
    return f"{v:,.1f} {units[i]}" if i else f"{int(v)} {units[i]}"


def format_speed(bps: float) -> str:
    if bps <= 0:
        return "0 B/s"
    return format_bytes(bps) + "/s"


def format_eta(seconds: Optional[float]) -> str:
    if seconds is None or seconds != seconds or seconds == float("inf"):
        return "--:--:--"
    s = max(0, int(seconds))
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_rate_setting(bps: float) -> str:
    return "unlimited" if bps <= 0 else format_speed(bps)


def sanitize_filename(name: str) -> str:
    name = unquote(name).strip().split("?")[0].split("#")[0]
    name = os.path.basename(name) or "download.bin"
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")
    return name[:200] or "download.bin"


def filename_from_response(url: str, headers: httpx.Headers) -> str:
    cd = headers.get("content-disposition", "")
    if cd:
        m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, re.I)
        if m:
            return sanitize_filename(unquote(m.group(1)))
        m = re.search(r'filename\s*=\s*"([^"]+)"', cd) or re.search(r"filename\s*=\s*([^;]+)", cd, re.I)
        if m:
            return sanitize_filename(m.group(1).strip().strip('"'))
    path = urlparse(url).path.rstrip("/")
    if path and "." in os.path.basename(path):
        return sanitize_filename(os.path.basename(path))
    return sanitize_filename(os.path.basename(path) or "download.bin")


def unique_path(directory: Path, name: str) -> Path:
    p = directory / name
    if not p.exists() and not Path(str(p) + PART_SUFFIX).exists():
        return p
    stem, suffix = os.path.splitext(name)
    i = 1
    while True:
        cand = directory / f"{stem} ({i}){suffix}"
        if not cand.exists() and not Path(str(cand) + PART_SUFFIX).exists():
            return cand
        i += 1


def render_bar(frac: float, width: int = 28) -> str:
    frac = max(0.0, min(1.0, frac if frac == frac else 0.0))
    filled = int(round(frac * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


# ---------------------------------------------------------------------------
# Speed limiter (token bucket, shared across threads)
# ---------------------------------------------------------------------------

class SpeedLimiter:
    def __init__(self, rate_bps: float = 0.0):
        self._lock = threading.Lock()
        self._rate = float(rate_bps)
        self._allowance = float(rate_bps) if rate_bps > 0 else 0.0
        self._last = time.monotonic()

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate

    def set_rate(self, rate_bps: float) -> None:
        with self._lock:
            self._rate = float(max(0.0, rate_bps))
            self._allowance = min(self._allowance, self._rate) if self._rate > 0 else 0.0
            self._last = time.monotonic()

    def wait(self, nbytes: int) -> None:
        """Block just enough to respect the global rate. No-op if unlimited."""
        if nbytes <= 0:
            return
        while True:
            with self._lock:
                rate = self._rate
                if rate <= 0:
                    return
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._allowance += elapsed * rate
                if self._allowance > rate:  # 1s burst cap
                    self._allowance = rate
                if self._allowance >= nbytes:
                    self._allowance -= nbytes
                    return
                deficit = nbytes - self._allowance
                sleep_for = deficit / rate
            time.sleep(min(sleep_for, 0.25))


# ---------------------------------------------------------------------------
# Throughput tracker (current speed via sliding window)
# ---------------------------------------------------------------------------

class Throughput:
    """Sliding-window byte counter. Keeps ~15s of samples for the sparkline;
    current speed is computed over the last ~3s."""

    SAMPLE_WINDOW = 15.0
    SPEED_WINDOW = 3.0

    def __init__(self, window: float = SAMPLE_WINDOW):
        self._lock = threading.Lock()
        self._samples: Deque[Tuple[float, int]] = deque()
        self._window = window
        self._total = 0

    def add(self, n: int) -> None:
        if n <= 0:
            return
        now = time.monotonic()
        with self._lock:
            self._samples.append((now, n))
            self._total += n
            cutoff = now - self._window
            while self._samples and self._samples[0][0] < cutoff:
                self._samples.popleft()

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    def current_speed(self) -> float:
        # Non-destructive: sum samples inside SPEED_WINDOW without popping,
        # so sparkline() keeps its longer history. add() prunes old entries.
        now = time.monotonic()
        with self._lock:
            cutoff = now - self.SPEED_WINDOW
            recent = [(ts, n) for ts, n in self._samples if ts >= cutoff]
            if not recent:
                return 0.0
            span = now - recent[0][0]
            if span < 0.2:
                span = 0.2
            return sum(n for _, n in recent) / span

    def sparkline(self, width: int = 24, bucket: float = 0.5) -> str:
        """Tiny speed-history graph, newest on the right. Pure ASCII/blocks."""
        levels = "_.-:=+x#@"
        now = time.monotonic()
        with self._lock:
            if not self._samples:
                return "_" * width
            buckets = [0] * width
            for ts, n in self._samples:
                age = now - ts
                if age < 0 or age >= width * bucket:
                    continue
                idx = width - 1 - int(age / bucket)
                buckets[idx] += n
        peak = max(buckets) or 1
        out = []
        for b in buckets:
            lvl = int(round(b / peak * (len(levels) - 1)))
            out.append(levels[lvl])
        return "".join(out)


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------

@dataclass
class ProbeResult:
    url: str
    final_url: str
    filename: str
    total: int  # 0 = unknown
    range_supported: bool
    etag: str = ""
    last_modified: str = ""
    content_type: str = ""


def probe_url(url: str, timeout: float, headers: Dict[str, str]) -> ProbeResult:
    """HEAD + Range probe to learn size, filename and resume capability."""
    with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as c:
        total = 0
        filename = ""
        etag = ""
        last_mod = ""
        ctype = ""
        try:
            r = c.head(url)
            if r.status_code < 400:
                total = int(r.headers.get("content-length", 0) or 0)
                etag = r.headers.get("etag", "")
                last_mod = r.headers.get("last-modified", "")
                ctype = r.headers.get("content-type", "")
                filename = filename_from_response(str(r.url), r.headers)
        except Exception:
            pass
        # Range check: authoritative
        range_supported = False
        try:
            rh = dict(headers)
            rh["Range"] = "bytes=0-0"
            with c.stream("GET", url, headers=rh) as rs:
                if rs.status_code == 206:
                    range_supported = True
                    cr = rs.headers.get("content-range", "")
                    m = re.search(r"bytes\s+\d+-\d+/(\d+|\*)", cr)
                    if m and m.group(1) != "*":
                        total = int(m.group(1))
                    if not filename:
                        filename = filename_from_response(str(rs.url), rs.headers)
                    etag = etag or rs.headers.get("etag", "")
                    last_mod = last_mod or rs.headers.get("last-modified", "")
                    ctype = ctype or rs.headers.get("content-type", "")
                    for _ in rs.iter_bytes(chunk_size=1):
                        break
                elif rs.status_code == 200:
                    if total == 0:
                        total = int(rs.headers.get("content-length", 0) or 0)
                    if not filename:
                        filename = filename_from_response(str(rs.url), rs.headers)
                    ctype = ctype or rs.headers.get("content-type", "")
                final_url = str(rs.url)
                # drain guard: closing context closes connection
        except Exception:
            final_url = url
        if not filename:
            filename = sanitize_filename(os.path.basename(urlparse(url).path) or "download.bin")
        return ProbeResult(url=url, final_url=final_url, filename=filename,
                           total=max(0, total), range_supported=range_supported,
                           etag=etag, last_modified=last_mod, content_type=ctype)


# ---------------------------------------------------------------------------
# Resume state
# ---------------------------------------------------------------------------

def state_path_for(dest: Path) -> Path:
    return Path(str(dest) + STATE_SUFFIX)


def part_path_for(dest: Path) -> Path:
    return Path(str(dest) + PART_SUFFIX)


def load_state(meta: Path) -> Optional[dict]:
    try:
        if meta.exists():
            return json.loads(meta.read_text())
    except Exception:
        pass
    return None


def save_state(meta: Path, data: dict) -> None:
    try:
        tmp = Path(str(meta) + ".tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, meta)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Single file download job
# ---------------------------------------------------------------------------

@dataclass
class Segment:
    index: int
    start: int
    end: int  # inclusive
    done: int = 0

    @property
    def size(self) -> int:
        return self.end - self.start + 1


@dataclass
class FileJob:
    url: str
    dest: Path
    total: int = 0
    downloaded: int = 0
    status: str = "queued"
    detail: str = ""
    retries: int = 0
    connections_used: int = 0
    segments: List[Segment] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    avg_speed: float = 0.0
    tracker: Throughput = field(default_factory=Throughput)  # per-file live speed


class Downloader:
    def __init__(self, out_dir: Path, connections: int, retries: int,
                 timeout: float, limiter: SpeedLimiter,
                 paused: threading.Event, stop_event: threading.Event,
                 throughput: Throughput, base_headers: Dict[str, str],
                 allow_segmented: bool = True):
        self.out_dir = out_dir
        self.connections = max(1, min(MAX_CONNECTIONS, connections))
        self.retries = retries
        self.timeout = timeout
        self.limiter = limiter
        self.paused = paused
        self.stop_event = stop_event
        self.throughput = throughput
        self.base_headers = base_headers
        self.allow_segmented = allow_segmented
        self._state_lock = threading.Lock()

    # -- segment worker ----------------------------------------------------
    def _fetch_range(self, url: str, part: Path, seg: Segment, job: FileJob) -> bool:
        attempt = 0
        while seg.done < seg.size:
            if self.stop_event.is_set():
                return False
            while self.paused.is_set() and not self.stop_event.is_set():
                time.sleep(0.1)
            if self.stop_event.is_set():
                return False
            offset = seg.start + seg.done
            headers = dict(self.base_headers)
            headers["Range"] = f"bytes={offset}-{seg.end}"
            try:
                with httpx.Client(headers=headers, timeout=self.timeout,
                                  follow_redirects=True) as client:
                    with client.stream("GET", url) as resp:
                        if resp.status_code not in (200, 206):
                            raise RuntimeError(f"HTTP {resp.status_code}")
                        with open(part, "r+b") as f:
                            pos = offset
                            for chunk in resp.iter_bytes(chunk_size=CHUNK_SIZE):
                                if self.stop_event.is_set():
                                    return False
                                while self.paused.is_set() and not self.stop_event.is_set():
                                    time.sleep(0.1)
                                if not chunk:
                                    continue
                                self.limiter.wait(len(chunk))
                                # clamp to segment end (server may ignore Range)
                                remaining = seg.end - pos + 1
                                if len(chunk) > remaining:
                                    chunk = chunk[:remaining]
                                f.seek(pos)
                                f.write(chunk)
                                pos += len(chunk)
                                seg.done += len(chunk)
                                with self._state_lock:
                                    job.downloaded += len(chunk)
                                self.throughput.add(len(chunk))
                                job.tracker.add(len(chunk))
                                if pos > seg.end:
                                    break
                return seg.done >= seg.size
            except Exception as exc:  # retry with backoff
                attempt += 1
                with self._state_lock:
                    job.retries += 1
                    job.detail = f"seg {seg.index} retry {attempt}: {exc}"
                if attempt > self.retries or self.stop_event.is_set():
                    job.detail = f"seg {seg.index} failed: {exc}"
                    return False
                time.sleep(min(2 ** attempt, 30) + (0.1 * attempt))

    def _fetch_single(self, url: str, part: Path, job: FileJob,
                      resume_from: int, total: int) -> bool:
        attempt = 0
        while True:
            if self.stop_event.is_set():
                return False
            while self.paused.is_set() and not self.stop_event.is_set():
                time.sleep(0.1)
            headers = dict(self.base_headers)
            start = resume_from + job.downloaded if total else resume_from
            if start > 0 or total:
                # resume attempt; server may ignore -> we handle below
                headers["Range"] = f"bytes={start}-"
            try:
                mode = "r+b" if part.exists() else "w+b"
                with httpx.Client(headers=headers, timeout=self.timeout,
                                  follow_redirects=True) as client:
                    with client.stream("GET", url) as resp:
                        if resp.status_code not in (200, 206):
                            raise RuntimeError(f"HTTP {resp.status_code}")
                        server_resumes = resp.status_code == 206
                        with open(part, mode) as f:
                            if resp.status_code == 200 and start > 0 and not server_resumes:
                                # server ignored Range: restart cleanly
                                f.seek(0)
                                f.truncate(0)
                                with self._state_lock:
                                    job.downloaded = 0
                                # recount: downloaded tracks session bytes for unknown total;
                                # for known total reset handled by caller via file size check
                                start = 0
                            else:
                                f.seek(start)
                            for chunk in resp.iter_bytes(chunk_size=CHUNK_SIZE):
                                if self.stop_event.is_set():
                                    return False
                                while self.paused.is_set() and not self.stop_event.is_set():
                                    time.sleep(0.1)
                                if not chunk:
                                    continue
                                self.limiter.wait(len(chunk))
                                f.write(chunk)
                                with self._state_lock:
                                    job.downloaded += len(chunk)
                                self.throughput.add(len(chunk))
                                job.tracker.add(len(chunk))
                return True
            except Exception as exc:
                attempt += 1
                with self._state_lock:
                    job.retries += 1
                    job.detail = f"retry {attempt}: {exc}"
                if attempt > self.retries or self.stop_event.is_set():
                    job.detail = f"failed: {exc}"
                    return False
                # refresh resume offset from disk
                try:
                    if part.exists():
                        disk = part.stat().st_size
                        with self._state_lock:
                            # job.downloaded counts session bytes; recompute start next loop
                            pass
                        resume_from = disk
                        with self._state_lock:
                            job.downloaded = 0 if total == 0 else max(0, disk - (job._base_offset if hasattr(job, "_base_offset") else 0))
                            # NOTE: for known-total single mode we store absolute below
                except Exception:
                    pass
                time.sleep(min(2 ** attempt, 30) + (0.1 * attempt))

    # -- main per-file routine ---------------------------------------------
    def download_one(self, url: str, probe: ProbeResult, dest: Path, job: FileJob) -> bool:
        job.start_time = time.monotonic()
        job.status = "downloading"
        part = part_path_for(dest)
        meta = state_path_for(dest)
        total = probe.total

        # fast path: final file already complete
        if dest.exists() and total and dest.stat().st_size == total:
            job.downloaded = total
            job.total = total
            job.status = "done"
            job.end_time = time.monotonic()
            return True

        use_segmented = (self.allow_segmented and probe.range_supported
                         and total >= MIN_SEGMENTED_SIZE and self.connections > 1)

        if use_segmented:
            return self._download_segmented(url, probe, dest, part, meta, job)
        return self._download_plain(url, probe, dest, part, meta, job)

    def _download_segmented(self, url, probe, dest, part, meta, job) -> bool:
        total = probe.total
        n = self.connections
        # build segments
        seg_size = total // n
        segments = []
        prior = load_state(meta)
        prior_done: List[int] = []
        valid_resume = False
        if prior and prior.get("url") == probe.final_url and prior.get("total") == total:
            if prior.get("etag", "") in ("", probe.etag) and part.exists() and part.stat().st_size == total:
                prior_done = prior.get("seg_done", [])
                valid_resume = len(prior_done) == n
        for i in range(n):
            s = i * seg_size
            e = total - 1 if i == n - 1 else (i + 1) * seg_size - 1
            d = prior_done[i] if valid_resume and i < len(prior_done) else 0
            d = max(0, min(d, e - s + 1))
            segments.append(Segment(i, s, e, d))
        job.segments = segments
        job.total = total
        job.connections_used = n
        with self._state_lock:
            job.downloaded = sum(s.done for s in segments)

        # pre-allocate
        try:
            if not part.exists() or part.stat().st_size != total:
                with open(part, "wb") as f:
                    f.truncate(total)
            else:
                # ensure size correct even on resume
                with open(part, "r+b") as f:
                    f.truncate(total)
        except Exception as exc:
            job.status = "failed"
            job.detail = f"disk error: {exc}"
            return False

        job.status = "downloading"
        last_save = time.monotonic()

        def persist(force=False):
            nonlocal last_save
            now = time.monotonic()
            if force or now - last_save >= STATE_SAVE_INTERVAL:
                save_state(meta, {"url": probe.final_url, "total": total,
                                  "etag": probe.etag, "filename": dest.name,
                                  "seg_done": [s.done for s in segments]})
                last_save = now

        todo = [s for s in segments if s.done < s.size]
        if not todo:
            return self._finalize(dest, part, meta, job)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n,
                                                   thread_name_prefix="seg") as ex:
            futs = {ex.submit(self._fetch_range, probe.final_url, part, s, job): s for s in todo}
            try:
                while futs:
                    if self.stop_event.is_set():
                        for f in futs:
                            f.cancel()
                        job.status = "stopped"
                        persist(force=True)
                        return False
                    done, _ = concurrent.futures.wait(list(futs), timeout=0.2)
                    for f in list(done):
                        s = futs.pop(f)
                        try:
                            ok = f.result()
                        except Exception as exc:
                            ok = False
                            job.detail = f"seg {s.index} error: {exc}"
                        if not ok and s.done < s.size and not self.stop_event.is_set():
                            # re-queue once more is handled inside worker retries;
                            # mark failure
                            job.status = "failed"
                    persist()
                    # live status text
                    if self.paused.is_set():
                        job.status = "paused"
                    elif job.status != "failed":
                        job.status = "downloading"
                # check completeness
                if all(s.done >= s.size for s in segments):
                    return self._finalize(dest, part, meta, job)
                job.status = "failed" if not self.stop_event.is_set() else "stopped"
                persist(force=True)
                return False
            finally:
                if job.status != "done":
                    persist(force=True)

    def _download_plain(self, url, probe, dest, part, meta, job) -> bool:
        total = probe.total
        job.total = total
        job.connections_used = 1
        prior = load_state(meta)
        resume_from = 0
        if part.exists():
            disk = part.stat().st_size
            if total and disk == total:
                return self._finalize(dest, part, meta, job)
            if probe.range_supported and disk > 0 and (not total or disk < total):
                if prior and prior.get("url") == probe.final_url:
                    resume_from = disk
                else:
                    resume_from = disk
            elif disk > 0 and not probe.range_supported:
                # cannot resume; restart
                try:
                    part.unlink()
                except Exception:
                    pass
                resume_from = 0
        job.downloaded = 0
        job._base_offset = resume_from  # type: ignore[attr-defined]
        job.status = "downloading"

        # background state saver not needed for plain; save offset occasionally
        ok = self._fetch_single_with_progress(probe.final_url, part, job, resume_from, total, meta, probe)
        if ok and not self.stop_event.is_set():
            # verify size when known
            try:
                if total and part.stat().st_size != total:
                    # server may have sent more/less; accept if >= total? else fail
                    if part.stat().st_size < total:
                        job.status = "failed"
                        job.detail = f"incomplete: got {part.stat().st_size}/{total}"
                        return False
            except Exception:
                pass
            return self._finalize(dest, part, meta, job)
        if self.stop_event.is_set():
            job.status = "stopped"
        elif job.status not in ("failed",):
            job.status = "failed"
        # save resume meta for plain mode
        try:
            disk = part.stat().st_size if part.exists() else 0
            save_state(meta, {"url": probe.final_url, "total": total,
                              "etag": probe.etag, "filename": dest.name,
                              "plain_offset": disk})
        except Exception:
            pass
        return False

    def _fetch_single_with_progress(self, url, part, job, resume_from, total, meta, probe) -> bool:
        # wraps _fetch_single but keeps job.total absolute progress correct
        base = resume_from
        # monkey-track: _fetch_single increments job.downloaded (session bytes).
        # Convert to absolute at the end of each save tick via display helper.
        # To keep UI simple, store session bytes and let renderer add base.
        ok = self._fetch_single(url, part, job, resume_from, total)
        if ok:
            with self._state_lock:
                # normalize downloaded to absolute for final display
                try:
                    disk = part.stat().st_size if part.exists() else base + job.downloaded
                except Exception:
                    disk = base + job.downloaded
                job.downloaded = disk if not total else disk
                # for known total plain mode absolute == disk
        else:
            # on failure keep absolute-ish value for resume display
            try:
                disk = part.stat().st_size if part.exists() else base
                with self._state_lock:
                    job.downloaded = disk
            except Exception:
                pass
        return ok

    def _finalize(self, dest: Path, part: Path, meta: Path, job: FileJob) -> bool:
        try:
            job.status = "verifying"
            # fsync part
            try:
                with open(part, "rb") as f:
                    os.fsync(f.fileno())
            except Exception:
                pass
            os.replace(part, dest)
            try:
                if meta.exists():
                    meta.unlink()
            except Exception:
                pass
            try:
                if job.total and dest.stat().st_size != job.total:
                    job.downloaded = dest.stat().st_size
                else:
                    job.downloaded = dest.stat().st_size
            except Exception:
                pass
            job.end_time = time.monotonic()
            elapsed = max(0.001, job.end_time - job.start_time)
            job.avg_speed = job.downloaded / elapsed
            job.status = "done"
            job.detail = ""
            return True
        except Exception as exc:
            job.status = "failed"
            job.detail = f"finalize error: {exc}"
            return False


# ---------------------------------------------------------------------------
# yt-dlp backend
# ---------------------------------------------------------------------------

def download_with_ytdlp(url: str, out_dir: Path, limiter: SpeedLimiter,
                        paused: threading.Event, stop_event: threading.Event,
                        job: FileJob, throughput: Throughput,
                        extra_args: Optional[List[str]] = None) -> bool:
    job.start_time = time.monotonic()
    job.status = "downloading"
    job.detail = "yt-dlp backend"
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["yt-dlp", "--newline", "--continue", "--no-overwrites",
           "--no-playlist", "-o", str(out_dir / "%(title)s [%(id)s].%(ext)s"), url]
    rate = limiter.rate
    if rate > 0:
        cmd[1:1] = ["--limit-rate", str(int(rate))]
    if extra_args:
        cmd[1:1] = extra_args
    job.connections_used = 1
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1)
    except FileNotFoundError:
        job.status = "failed"
        job.detail = "yt-dlp not installed (pip install yt-dlp)"
        return False

    def pump():
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if "[download]" in line and "%" in line:
                m = re.search(r"(\d+\.?\d*)%", line)
                if m:
                    try:
                        pct = float(m.group(1))
                        job.detail = line[-90:]
                        # rough progress: keep bar moving; exact size unknown
                        job.total = job.total or 1000
                        target = int(pct / 100 * job.total)
                        delta = max(0, target - job.downloaded)
                        if delta:
                            job.downloaded += delta
                            throughput.add(delta)
                            job.tracker.add(delta)
                    except Exception:
                        pass
            elif line:
                job.detail = line[-90:]
            if stop_event.is_set():
                break

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    # pause support via SIGSTOP/SIGCONT on POSIX
    try:
        while proc.poll() is None:
            if stop_event.is_set():
                proc.terminate()
                break
            if paused.is_set():
                try:
                    if os.name == "posix":
                        proc.send_signal(signal.SIGSTOP)
                        while paused.is_set() and proc.poll() is None and not stop_event.is_set():
                            time.sleep(0.1)
                        if proc.poll() is None:
                            proc.send_signal(signal.SIGCONT)
                    else:
                        time.sleep(0.2)
                    job.status = "paused" if paused.is_set() else "downloading"
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
    if rc == 0 and not stop_event.is_set():
        job.status = "done"
        job.detail = ""
        return True
    if stop_event.is_set():
        job.status = "stopped"
    else:
        job.status = "failed"
        job.detail = f"yt-dlp exit {rc}"
    return False


# ---------------------------------------------------------------------------
# Keyboard controls
# ---------------------------------------------------------------------------

def handle_key(ch: str, paused: threading.Event, limiter: SpeedLimiter,
               stop_event: threading.Event) -> None:
    """Apply one keypress. Shared by the POSIX and Windows listeners."""
    if not ch:
        return
    c = ch.lower()
    if c == "p" or ch == " ":
        paused.clear() if paused.is_set() else paused.set()
    elif c == "r":
        paused.clear()
    elif c == "+":
        cur = limiter.rate
        base = cur if cur > 0 else 1024 * 1024
        limiter.set_rate(min(base * 2, 1024 ** 4))
    elif c == "-" or ch == "_":
        cur = limiter.rate
        if cur <= 0:
            limiter.set_rate(512 * 1024)
        else:
            nxt = cur / 2
            limiter.set_rate(0 if nxt < 32 * 1024 else nxt)
    elif c == "u":
        limiter.set_rate(0)
    elif c == "q":
        stop_event.set()


def keyboard_loop(paused: threading.Event, limiter: SpeedLimiter,
                  stop_event: threading.Event) -> None:
    """Background key listener.

    Reads from /dev/tty (the controlling terminal), NOT stdin, so hotkeys
    keep working when stdin is a pipe, e.g. `pbpaste | dl`. Falls back to
    stdin only when it is a TTY and /dev/tty is unavailable. Silent no-op
    when there is no terminal at all (cron, background job).
    """
    if os.name == "posix":
        import termios
        import tty
        fd = -1
        opened_tty = False
        try:
            fd = os.open("/dev/tty", os.O_RDONLY)
            opened_tty = True
        except Exception:
            try:
                if sys.stdin.isatty():
                    fd = sys.stdin.fileno()
                else:
                    return
            except Exception:
                return
        try:
            try:
                old = termios.tcgetattr(fd)
            except Exception:
                if opened_tty:
                    os.close(fd)
                return
            try:
                tty.setcbreak(fd)
                while not stop_event.is_set():
                    r, _, _ = select.select([fd], [], [], 0.15)
                    if not r:
                        continue
                    try:
                        ch = os.read(fd, 1).decode("utf-8", "ignore")
                    except Exception:
                        continue
                    handle_key(ch, paused, limiter, stop_event)
                    if stop_event.is_set() and ch.lower() == "q":
                        break
            finally:
                try:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)
                except Exception:
                    pass
        finally:
            if opened_tty:
                try:
                    os.close(fd)
                except Exception:
                    pass
    else:
        # Windows fallback using msvcrt
        try:
            import msvcrt
        except ImportError:
            return
        while not stop_event.is_set():
            if msvcrt.kbhit():
                try:
                    ch = msvcrt.getch().decode("utf-8", "ignore").lower()
                except Exception:
                    time.sleep(0.1)
                    continue
                handle_key(ch, paused, limiter, stop_event)
            else:
                time.sleep(0.15)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def short_name(name: str, width: int = 34) -> str:
    if len(name) <= width:
        return name
    return name[: width - 3] + "..."


def seg_mini_bar(frac: float, width: int = 7) -> str:
    """Compact per-segment bar, e.g. S0[###----]."""
    frac = max(0.0, min(1.0, frac if frac == frac else 0.0))
    filled = int(round(frac * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def build_ui(jobs: List[FileJob], throughput: Throughput, limiter: SpeedLimiter,
             paused: threading.Event, started: float, current_idx: int,
             total_urls: int, term_width: int = 0,
             mini_art=None) -> Group:
    is_paused = paused.is_set()
    # Text budget for the session grid: truncating value strings to this
    # keeps every row on exactly one line, so render height is constant.
    budget = max(40, (term_width or 120) - 30)
    # Budget: fixed columns + chrome (~19) must stay near ~90 so the table
    # is stable on >=100-col terminals; File flexes, everything else is
    # frozen. Conn lives in the Detail row, not as a column.
    table = Table(show_header=True, header_style="bold cyan", expand=True)
    table.add_column("File", style="white", no_wrap=True, ratio=1,
                     max_width=36, overflow="ellipsis")
    table.add_column("Size", justify="right", style="white", no_wrap=True,
                     width=10, overflow="ellipsis")
    table.add_column("Progress", justify="left", no_wrap=True,
                     width=30, overflow="ellipsis")
    table.add_column("Speed", justify="right", style="green", no_wrap=True,
                     width=11, overflow="ellipsis")
    table.add_column("ETA", justify="right", style="yellow", no_wrap=True,
                     width=8, overflow="ellipsis")
    table.add_column("Status", justify="center", no_wrap=True,
                     width=11, overflow="ellipsis")

    now = time.monotonic()
    active_job: Optional[FileJob] = None
    for j in jobs:
        total = j.total
        done = j.downloaded
        frac = (done / total) if total else 0.0
        pct = frac * 100 if total else 0.0
        # Two-line fixed-shape cell: padded constant so the Progress column
        # never changes width between frames (bar 20 -> line1 is 29 chars).
        bar = render_bar(frac, 20)
        if total:
            prog = (f"{bar} {pct:5.1f}%\n"
                    f"{format_bytes(done):>10}/{format_bytes(total):<10}")
        else:
            prog = (f"{' ' * 22} {' ' * 6}\n"
                    f"{format_bytes(done):>10} downloaded")
        live = j.status in ("downloading", "paused")
        if live and active_job is None:
            active_job = j
        # Active marker folded into the File cell (constant 2-char prefix).
        # Text() wrapper also shields filenames containing [ ] from markup.
        fname = Text(no_wrap=True)
        fname.append("> " if live else "  ",
                     style="bold cyan" if live else "dim")
        fname.append(short_name(j.dest.name))
        spd = ""
        eta = "--:--:--"
        if live:
            js = j.tracker.current_speed()
            spd = format_speed(js)
            if total and js > 0:
                eta = format_eta((total - done) / js)
        elif j.status == "done":
            spd = format_speed(j.avg_speed) if j.avg_speed else "-"
            eta = "00:00:00"
        status_style = {"downloading": "green", "paused": "yellow", "done": "bold green",
                        "failed": "bold red", "queued": "dim", "verifying": "cyan",
                        "stopped": "red", "retrying": "yellow"}.get(j.status, "white")
        status_txt = Text(j.status.upper(), style=status_style)
        size_txt = format_bytes(total) if total else "unknown"
        # NOTE: wrap prog in Text() so the bar's [ ] are not eaten as markup
        table.add_row(fname, size_txt, Text(prog), spd, eta, status_txt)

    elapsed = now - started
    cur_speed = throughput.current_speed()
    spark = throughput.sparkline()
    done_count = sum(1 for j in jobs if j.status == "done")
    fail_count = sum(1 for j in jobs if j.status == "failed")

    # overall batch progress (bytes, when sizes are known)
    known_total = sum(j.total for j in jobs if j.total)
    known_done = sum(min(j.downloaded, j.total) for j in jobs if j.total)
    if known_total:
        ofrac = known_done / known_total
        overall = (f"{render_bar(ofrac, 20)} {ofrac * 100:5.1f}%  "
                   f"{format_bytes(known_done)}/{format_bytes(known_total)}")
    else:
        overall = f"{format_bytes(throughput.total)} transferred"

    # active-job segment detail with mini bars (capped: stable panel height)
    if active_job is not None and active_job.segments:
        segs = []
        for s in active_job.segments[:8]:
            p = (s.done / s.size) if s.size else 0.0
            segs.append(f"S{s.index}{seg_mini_bar(p, width=4)}")
        seg_line = f"conn {active_job.connections_used} | " + " ".join(segs)
        if active_job.detail:
            seg_line += f" | {active_job.detail[-40:]}"
        if len(active_job.segments) > 8:
            seg_line += f" (+{len(active_job.segments) - 8} more)"
        seg_line = seg_line[:150]
    elif active_job is not None and active_job.detail:
        seg_line = active_job.detail[-100:]
    else:
        seg_line = "idle"

    if is_paused:
        state = Text("PAUSED - press R to resume", style="bold yellow")
    else:
        state = Text("RUNNING", style="green")
    session_txt = (f"elapsed {format_eta(elapsed)}   speed {format_speed(cur_speed)} "
                   f"[{spark}]   total {format_bytes(throughput.total)}   "
                   f"limit {format_rate_setting(limiter.rate)}")[:budget]
    batch_txt = (f"file {min(current_idx + 1, total_urls)}/{total_urls}   "
                 f"done {done_count}   failed {fail_count}   state ")[:budget]
    summary = Table.grid(padding=(0, 2))
    summary.add_column(no_wrap=True)
    summary.add_column(no_wrap=True, overflow="ellipsis", max_width=budget)
    summary.add_row("Session:", session_txt)
    summary.add_row("Batch:", batch_txt)
    summary.add_row("", state)
    summary.add_row("Overall:", Text(overall[:budget]))
    summary.add_row("Detail:", Text(seg_line[:budget]))

    help_txt = Text("Keys: [P/Space] pause-resume   [R] resume   [+/-] speed limit   "
                    "[U] unlimited   [Q] quit (resume later)", style="dim")
    title_line = f"{APP_NAME} v{APP_VERSION}  |  sequential batch, segmented engine"
    if is_paused:
        title_line += "  |  PAUSED"
    logo_block = Group(Text(LOGO, style="bold cyan", no_wrap=True), Text(""))
    mini = _mini_text(mini_art, term_width or 0)
    if mini is None:
        header_body = Group(logo_block, Text(title_line, style="bold white"))
    else:
        header_body = Group(
            Columns([logo_block, mini], padding=(0, 3), expand=False),
            Text(title_line, style="bold white"),
        )
    header = Panel(header_body,
                   style="bold yellow" if is_paused else "bold blue")
    return Group(header, table,
                 Panel(summary, title="Session - PAUSED" if is_paused else "Session",
                       border_style="yellow" if is_paused else "blue"),
                 Panel(help_txt, border_style="dim"))


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def load_urls(args) -> List[str]:
    urls: List[str] = list(args.url or [])
    if args.file:
        p = Path(args.file)
        if not p.exists():
            print(f"URL file not found: {p}", file=sys.stderr)
            sys.exit(2)
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    # dedupe preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def looks_like_video(url: str) -> bool:
    u = url.lower()
    return any(h in u for h in VIDEO_HINTS)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Fast segmented link downloader with pause, speed limit and resume.")
    ap.add_argument("url", nargs="*", help="One or more download URLs")
    ap.add_argument("-f", "--file", help="Text file with one URL per line")
    ap.add_argument("-o", "--output-dir", default=str(Path.cwd()),
                    help="Output directory (default: current dir)")
    ap.add_argument("-c", "--connections", type=int, default=DEFAULT_CONNECTIONS,
                    help=f"Connections per file 1-{MAX_CONNECTIONS} (default {DEFAULT_CONNECTIONS})")
    ap.add_argument("--speed-limit", type=parse_rate, default="0",
                    help="Global speed limit: 0=unlimited, e.g. 500K, 5M (default 0)")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retries per segment (default 5)")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds (default 30)")
    ap.add_argument("--yt-dlp", action="store_true", help="Force yt-dlp backend for all URLs")
    ap.add_argument("--no-segmented", action="store_true", help="Disable segmented downloads")
    ap.add_argument("--yt-dlp-args", default="", help="Extra args forwarded to yt-dlp (quoted string)")
    ap.add_argument("--banner", type=int, default=None,
                    choices=range(1, len(_BANNERS) + 1) if _BANNERS else None,
                    metavar="1-%d" % len(_BANNERS) if _BANNERS else "N",
                    help="Force startup splash art (default: random one that fits)")
    ap.add_argument("--no-banner", action="store_true", help="Skip the startup splash art")
    ap.add_argument("--no-screen", action="store_true",
                    help="Disable alternate-screen mode (inline UI, e.g. for logging)")
    return ap.parse_args(argv)


def prompt_for_urls() -> List[str]:
    """Interactively ask the user to paste links (used by the `dl` shortcut)."""
    print("Paste download link(s), one per line. Empty line to start downloading.")
    print("Tip: you can also run: dl <url> [url ...]  or  dl -f urls.txt")
    collected: List[str] = []
    while True:
        try:
            line = input("link> ").strip()
        except EOFError:
            break
        if not line:
            break
        if line.lower() in ("done", "go", "start"):
            break
        # allow several URLs pasted on one line separated by space/comma
        for tok in re.split(r"[\s,]+", line):
            tok = tok.strip().strip("'\"")
            if tok:
                collected.append(tok)
    # dedupe preserving order
    seen = set()
    out = []
    for u in collected:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def read_piped_urls() -> List[str]:
    """Read URLs from piped stdin, e.g. `pbpaste | dl` or `cat urls.txt | dl`."""
    try:
        data = sys.stdin.read()
    except Exception:
        return []
    urls: List[str] = []
    for line in data.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            for tok in re.split(r"[\s,]+", line):
                tok = tok.strip().strip("'\"")
                if tok:
                    urls.append(tok)
    return urls


def main(argv=None) -> int:
    args = parse_args(argv)
    urls = load_urls(args)
    if not urls:
        if not sys.stdin.isatty():
            urls = read_piped_urls()
        else:
            urls = prompt_for_urls()
    if not urls:
        print("No URLs given. Provide URLs, -f urls.txt, paste them when asked, or pipe them via stdin.", file=sys.stderr)
        return 2

    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    limiter = SpeedLimiter(float(args.speed_limit))
    paused = threading.Event()
    stop_event = threading.Event()
    throughput = Throughput()
    console = Console()
    base_headers = {"User-Agent": USER_AGENT, "Accept": "*/*",
                    "Accept-Encoding": "identity", "Connection": "keep-alive"}

    # graceful Ctrl+C
    def on_sigint(signum, frame):
        if not stop_event.is_set():
            console.print("\n[red]Interrupt received - saving resume state...[/red]")
            stop_event.set()

    try:
        signal.signal(signal.SIGINT, on_sigint)
    except Exception:
        pass

    kb = threading.Thread(target=keyboard_loop, args=(paused, limiter, stop_event), daemon=True)
    kb.start()

    downloader = Downloader(out_dir, args.connections, args.retries, args.timeout,
                            limiter, paused, stop_event, throughput, base_headers,
                            allow_segmented=not args.no_segmented)

    jobs: List[FileJob] = []
    started = time.monotonic()
    # One banner identity per run: splash and header mini share it.
    splash_art, splash_rc = resolve_splash(console, args)
    if splash_rc:
        return splash_rc
    show_splash(console, splash_art)
    console.print(Text(LOGO, style="bold cyan", no_wrap=True))
    console.print(f"[bold blue]{APP_NAME} v{APP_VERSION}[/bold blue] - {len(urls)} URL(s) -> {out_dir}")
    console.print(f"Connections: {args.connections}  Limit: {format_rate_setting(limiter.rate)}  "
                  f"Retries: {args.retries}  Resume: enabled (.part + .part.json)")

    overall_ok = True

    with Live(build_ui(jobs, throughput, limiter, paused, started, 0, len(urls),
                       term_width=console.width, mini_art=splash_art),
              console=console, refresh_per_second=4, transient=False,
              screen=not args.no_screen) as live:
        def refresh(idx: int):
            try:
                live.update(build_ui(jobs, throughput, limiter, paused, started, idx, len(urls),
                                     term_width=console.width, mini_art=splash_art))
            except Exception:
                pass

        for idx, url in enumerate(urls):
            if stop_event.is_set():
                break
            url = url.strip()
            if not url:
                continue
            use_ytdlp = args.yt_dlp or looks_like_video(url)
            job: FileJob
            if use_ytdlp:
                dest = out_dir  # yt-dlp decides filename via template
                job = FileJob(url=url, dest=Path(f"yt-dlp [{idx + 1}] {url[:40]}"))
                jobs.append(job)
                ok = download_with_ytdlp(url, out_dir, limiter, paused, stop_event,
                                         job, throughput,
                                         extra_args=args.yt_dlp_args.split() if args.yt_dlp_args else None)
                # keep UI alive during yt-dlp via refresh ticks
                refresh(idx)
                if not ok:
                    overall_ok = False
                continue

            # probe
            tmp_job = FileJob(url=url, dest=Path("probing..."))
            tmp_job.status = "queued"
            # show placeholder while probing
            jobs.append(tmp_job)
            refresh(idx)
            try:
                probe = probe_url(url, args.timeout, base_headers)
            except Exception as exc:
                tmp_job.status = "failed"
                tmp_job.detail = str(exc)
                tmp_job.dest = Path(sanitize_filename(url[:40]))
                overall_ok = False
                refresh(idx)
                continue
            # HTML page without Range and not video -> hint yt-dlp
            if ("text/html" in (probe.content_type or "") and not probe.range_supported
                    and probe.total == 0 and looks_like_video(url)):
                tmp_job.status = "failed"
                tmp_job.detail = "page URL, retry with --yt-dlp"
                overall_ok = False
                refresh(idx)
                continue

            candidate = out_dir / probe.filename
            if (probe.total and candidate.exists() and candidate.is_file()
                    and candidate.stat().st_size == probe.total
                    and not part_path_for(candidate).exists()):
                dest = candidate  # already complete: fast-path skip in download_one
            else:
                dest = unique_path(out_dir, probe.filename)
            # replace placeholder with real job (preserve position)
            real = FileJob(url=url, dest=dest, total=probe.total)
            jobs[idx if len(jobs) > idx else -1] = real
            job = real
            refresh(idx)

            # run download in worker thread so UI keeps refreshing
            done_evt = threading.Event()
            result = {"ok": False}

            def run():
                try:
                    result["ok"] = downloader.download_one(probe.final_url, probe, dest, job)
                except Exception as exc:
                    job.status = "failed"
                    job.detail = str(exc)[:120]
                    result["ok"] = False
                finally:
                    done_evt.set()

            t = threading.Thread(target=run, daemon=True)
            t.start()
            # Content swap only; actual redraws happen on the 4fps auto tick.
            while not done_evt.is_set():
                if paused.is_set() and job.status == "downloading":
                    job.status = "paused"
                elif not paused.is_set() and job.status == "paused":
                    job.status = "downloading"
                refresh(idx)
                time.sleep(0.25)
            refresh(idx)
            if not result["ok"] and not stop_event.is_set():
                overall_ok = False
            if stop_event.is_set():
                break

        refresh(len(urls) - 1 if urls else 0)

    # final summary
    elapsed = time.monotonic() - started
    table = Table(title="Download summary", show_header=True, header_style="bold cyan")
    table.add_column("File")
    table.add_column("Size", justify="right")
    table.add_column("Status", justify="center")
    table.add_column("Avg speed", justify="right")
    table.add_column("Detail")
    for j in jobs:
        style = "green" if j.status == "done" else ("red" if j.status == "failed" else "yellow")
        table.add_row(short_name(j.dest.name if isinstance(j.dest, Path) else str(j.dest), 40),
                      format_bytes(j.total) if j.total else format_bytes(j.downloaded),
                      Text(j.status.upper(), style=style),
                      format_speed(j.avg_speed) if j.status == "done" else "-",
                      (j.detail or "")[-60:])
    console.print(table)
    console.print(f"Elapsed {format_eta(elapsed)}  Total {format_bytes(throughput.total)}  "
                  f"Avg {format_speed(throughput.total / max(elapsed, 0.001))}  "
                  f"Limit {format_rate_setting(limiter.rate)}")
    if stop_event.is_set():
        console.print("[yellow]Stopped. Re-run the same command to resume from saved state.[/yellow]")
        return 130
    if not overall_ok:
        console.print("[red]Completed with failures. Retry the same command to resume failed segments.[/red]")
        return 1
    console.print("[green]All downloads completed.[/green]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
