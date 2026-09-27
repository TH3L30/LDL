# LDL — L30 Download Manager

High-performance segmented file downloader with Rich terminal UI.

## Files

- `downloader.py` — main downloader (segmented HTTP Range downloads, speed limit, pause/resume, batch mode, optional yt-dlp backend)
- `banners.py` — startup banner gallery (braille-art splash, no dependencies)

## Requirements

- Python 3.10+
- `pip install -r requirements.txt`

## Usage

```bash
python downloader.py <url> [url ...] -o ~/Downloads
python downloader.py -f urls.txt -o ~/Downloads --connections 16 --speed-limit 5M
python downloader.py "https://youtube.com/watch?v=..." --yt-dlp
```

Controls while downloading (when stdin is a TTY):

- `P` or `Space`: pause / resume toggle
- `R`: resume
- `+` / `-`: increase / decrease speed limit
- `U`: unlimited speed
- `Q` or `Ctrl+C`: graceful quit (state saved, resume on next run)

## Features

- Multi-connection segmented HTTP downloads (Range requests)
- Single-stream fallback for servers without Range support / unknown size
- Global speed limit (token bucket), adjustable at runtime
- Pause / resume with keyboard controls, persistent resume across restarts
- Exponential-backoff retry, per-segment retry and re-split
- Batch mode: multiple URLs, one per line in a text file
- Optional yt-dlp backend for video / site URLs
- Detailed Rich terminal UI
