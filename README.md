# ⚡ FastTube Pro

**FastTube Pro** is a comprehensive, universal media downloader and converter desktop application built with Python & Tkinter. It features a modern dark UI, a local subscription system, and support for multiple platforms including YouTube, Instagram, Facebook, Twitter/X, and MX Player.

---

##  Features

### 1. 📦 Batch Downloader
- Download playlists or multiple URLs simultaneously.
- Queue management: Pause, Resume, Retry, and Cancel individual jobs.
- **Smart Rename:** Automatically sequence files based on playlist order.

### 2. ⬇ Single Video Downloader
- Supports **YouTube, Instagram, Facebook, Twitter, MX Player**, and 1000+ sites.
- Select specific resolutions (up to 4K/8K) and formats.
- Platform-specific optimizations (e.g., MX Player headers).

### 3. 🎵 Audio Extractor
- Extract audio in **MP3 (320kbps), WAV, FLAC, M4A, OPUS, AAC**.
- Options to embed thumbnails and retain metadata.

### 4. 🔄 Format Converter
- Convert local video files or download-and-convert URLs directly.
- Supports **MP4, MKV, MOV, WebM, AVI, FLV**.
- Adjustable video/audio bitrate and compression presets (High, Balanced, Compressed).

### 5. 📄 Subtitle Extractor
- Download subtitles in **SRT, VTT, TXT, ASS**.
- Supports both auto-generated and manual/embedded captions.

### 6. ℹ Metadata Studio
- **Viewer:** Inspect deep video details (Codecs, Bitrate, FPS, Upload Date).
- **Export:** Save metadata as JSON, CSV, or TXT.
- **Compare:** Side-by-side comparison of two videos.

### 7. 🖼 Thumbnail Downloader
- Fetch highest resolution thumbnails (maxresdefault).
- Bulk download support.

---

## ✅ Requirements

- **Python**: 3.10+
- **FFmpeg**: Required for merging video/audio and format conversion.
- **yt-dlp**: Core downloading engine.

---

## 🚀 Installation

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 3. Install FFmpeg
- **Windows**: Download from [ffmpeg.org](https://ffmpeg.org/download.html), extract, add `bin/` to PATH.
- **macOS**: `brew install ffmpeg`
- **Linux**: `sudo apt install ffmpeg`

Verify: `ffmpeg -version`

---

## ▶️ Run the App

```bash
python fasttube.py
```

---

## 🖥️ How to Use

1. Paste a YouTube URL into the input field.
2. Select **Video** or **Audio**.
3. Click **Fetch Options** — available qualities will appear.
4. Choose a quality from the dropdown.
5. (Optional) Click **Browse** to change the save folder.
6. Click **Download** — progress bar shows status.
7. File is saved in the `downloads/` folder (or your chosen folder).

---

## 📦 Build Executable

### Windows
```bat
scripts\build_windows.bat
```
Output: `dist\FastTube.exe`

### macOS / Linux
```bash
chmod +x scripts/build_unix.sh
./scripts/build_unix.sh
```
Output: `dist/FastTube`

> **Note:** FFmpeg must be installed on the target machine. Rebuild after updating `yt-dlp`.

---

## 🎵 Audio Quality Options

| Label | Format | Bitrate |
|-------|--------|---------|
| Fast M4A | M4A (native) | ~70K |
| Classic MP3 | MP3 | 128K |
| MP3 | MP3 | 160K |
| MP3 Best | MP3 | 320K |

## 🎬 Video Quality Options

`144p` · `240p` · `360p` · `480p` · `720p` · `1080p` · `1440p` · `2160p`

---

## ⚠️ Disclaimer

> Downloading content from YouTube may violate its Terms of Service, especially for copyrighted material.
> This app is **for educational and personal use only**.
> Always respect copyright laws. The developers are not responsible for any misuse.

---

## 🔧 Troubleshooting

| Problem | Fix |
|---------|-----|
| Options not loading | Check URL or internet connection |
| Audio not converting | Ensure FFmpeg is in PATH |
| .exe not working | Make sure FFmpeg is installed on that machine |
| yt-dlp broken | Run `pip install -U yt-dlp` and rebuild |

---

## 📄 License

MIT License — free to use, modify, and distribute.
