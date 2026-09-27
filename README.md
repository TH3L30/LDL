# LDL — L30 Download Manager

High-performance segmented file downloader with Rich terminal UI, powered by
[aria2](https://aria2.github.io/) (`aria2c`) as the default engine.

## Files

- `downloader.py` — main CLI + Rich UI (engine router: aria2 / native / yt-dlp)
- `aria2_backend.py` — wrapper around the `aria2c` binary (progress parsing, pause/resume, torrents/metalinks)
- `banners.py` — startup banner gallery (braille-art splash, no dependencies)

## Requirements

- Python 3.10+
- `pip install -r requirements.txt`
- `aria2c` binary for the default engine:
  - macOS: `brew install aria2`
  - Debian/Ubuntu: `apt install aria2`

Without `aria2c`, LDL falls back to the built-in native (httpx) engine.

## Usage

```bash
python downloader.py <url> [url ...] -o ~/Downloads
python downloader.py -f urls.txt -o ~/Downloads --connections 16 --speed-limit 5M
python downloader.py "https://youtube.com/watch?v=..." --yt-dlp

# engine selection (default: auto = aria2 if installed, else native)
python downloader.py <url> --engine aria2 --connections 16
python downloader.py <url> --engine native
python downloader.py <url> --aria2-args "--seed-ratio 1.0 --check-integrity=true"

# torrents / magnets / metalinks go through aria2 natively
python downloader.py "magnet:?xt=urn:btih:..." -o ~/Downloads
python downloader.py file.torrent -o ~/Downloads
```

Controls while downloading (when stdin is a TTY):

- `P` or `Space`: pause / resume toggle
- `R`: resume
- `+` / `-`: increase / decrease speed limit (native engine live; aria2 applies per-file)
- `U`: unlimited speed
- `Q` or `Ctrl+C`: graceful quit (aria2 `-c` resumes next run; native saves `.part` + `.part.json`)

## Engines

- **aria2 (default in auto mode)**: segmented HTTP(S)/FTP/SFTP (`-x`/`-s`/`-k`
  mapped from `--connections`), resume with `-c`, BitTorrent/magnet, Metalink,
  `--max-overall-download-limit` from `--speed-limit`. Extra flags via `--aria2-args`.
- **native**: built-in httpx segmented engine (Range requests, single-stream
  fallback, token-bucket limit, `.part` resume). Forced with `--engine native`
  or when `aria2c` is missing.
- **yt-dlp**: for video / site URLs (`--yt-dlp` or auto-detected video pages).

## Features

- Multi-connection segmented downloads (aria2 engine + native fallback)
- BitTorrent / magnet / Metalink support (aria2)
- Global speed limit, adjustable at runtime
- Pause / resume with keyboard controls, persistent resume across restarts
- Exponential-backoff retry
- Batch mode: multiple URLs, one per line in a text file
- Optional yt-dlp backend for video / site URLs
- Detailed Rich terminal UI
