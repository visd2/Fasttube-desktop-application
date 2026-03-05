"""
FastTube Pro — Universal Media Downloader
==========================================
Tabs:
  1. Batch Downloader  — Playlist/Multiple URLs, Queue, Pause/Resume, Retry, History
  2. Single Downloader — YouTube, Instagram, MX Player, Facebook, Twitter, +1000 sites
  3. Audio Extractor   — MP3/WAV/FLAC/M4A, 320kbps, Metadata, Thumbnail embed
  4. Format Converter  — MP4/MKV/MOV/WebM, Bitrate, Presets
  5. Subtitle Extractor— Embedded + auto subs, SRT/VTT/TXT, Language selector
  6. Metadata Viewer   — Title, Channel, Duration, Views, Upload date, Thumbnail
  7. Thumbnail DL      — Highest resolution, Bulk (saves as .jpg, NOT video)

Platforms: YouTube · Instagram · Facebook · Twitter/X · WhatsApp · Snapchat · MX Player
Default save: ~/Downloads/FastTube
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
import os
import re
import time
import shutil
import platform as _platform
import subprocess
import sys
import sqlite3
import urllib.request
import json
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import yt_dlp

# ══════════════════════════════════════════════════════════════════════════════
#  COLORS
# ══════════════════════════════════════════════════════════════════════════════
BG       = "#0f0f0f"
BG_CARD  = "#1a1a1a"
BG_INPUT = "#242424"
ACCENT   = "#e53935"
AHOVER   = "#c62828"
WHITE    = "#ffffff"
MUTED    = "#888888"
DIM      = "#3a3a3a"
BORDER   = "#2a2a2a"
GREEN    = "#00c853"
ORANGE   = "#ff6d00"
YELLOW   = "#ffab00"
BLUE     = "#2196f3"
PINK     = "#c13584"

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
SOCK_TO  = 60
MAX_RET  = 5
FRAG_RET = 15
CHUNK    = 10 * 1024 * 1024
C_FRAG   = 4
RET_SLP  = 3

# ══════════════════════════════════════════════════════════════════════════════
#  PLATFORMS
# ══════════════════════════════════════════════════════════════════════════════
PLATFORMS = {
    "youtube":   {"name": "YouTube",   "icon": "▶",  "color": "#e53935",
                  "domains": ["youtube.com","youtu.be","youtube-nocookie.com"]},
    "instagram": {"name": "Instagram", "icon": "📸", "color": "#c13584",
                  "domains": ["instagram.com","instagr.am"]},
    "facebook":  {"name": "Facebook",  "icon": "👤", "color": "#1877f2",
                  "domains": ["facebook.com","fb.com","fb.watch","m.facebook.com"]},
    "twitter":   {"name": "Twitter/X", "icon": "🐦", "color": "#1da1f2",
                  "domains": ["twitter.com","x.com","t.co"]},
    "whatsapp":  {"name": "WhatsApp",  "icon": "💬", "color": "#25d366",
                  "domains": ["whatsapp.com","wa.me"]},
    "snapchat":  {"name": "Snapchat",  "icon": "👻", "color": "#fffc00",
                  "domains": ["snapchat.com","snap.com"]},
    "mxplayer":  {"name": "MX Player", "icon": "🎯", "color": "#ff6b00",
                  "domains": ["mxplayer.in","mxmediaplayer.com","mxshare.in",
                              "mxplay.in","api.mxplayer.in"]},
}

PLAYER_CLIENTS = [
    ("TV Embedded", ["tv_embedded"]),
    ("Web",         ["web"]),
    ("Android",     ["android"]),
    ("iOS",         ["ios"]),
    ("mWeb",        ["mweb"]),
]

def detect_platform(url):
    u = url.lower()
    # MX Player — match domain AND common CDN/API patterns
    if any(x in u for x in ["mxplayer.in","mxplay.in","mxshare.in",
                              "mxmediaplayer","api.mxplayer"]):
        return "mxplayer"
    for key, info in PLATFORMS.items():
        for d in info["domains"]:
            if d in u:
                return key
    return "other"

def get_platform_ydl_opts_extra(pkey, base_opts):
    """Per-platform extra yt-dlp options."""
    if pkey == "mxplayer":
        # MX Player needs desktop Chrome UA — mobile UA gets blocked on some content
        base_opts["http_headers"] = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"),
            "Referer":    "https://www.mxplayer.in/",
            "Origin":     "https://www.mxplayer.in",
            "Accept":     "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        # MX Player uses HLS/DASH streams — allow all formats
        base_opts["format"]        = "bestvideo+bestaudio/best"
        base_opts["hls_prefer_native"] = False
    return base_opts

# ══════════════════════════════════════════════════════════════════════════════
#  FFMPEG
# ══════════════════════════════════════════════════════════════════════════════
def find_ffmpeg():
    if getattr(sys, "frozen", False):
        b = os.path.join(sys._MEIPASS, "ffmpeg.exe")
        if os.path.exists(b):
            return b
    exe_dir = (os.path.dirname(sys.executable)
               if getattr(sys, "frozen", False)
               else os.path.dirname(os.path.abspath(__file__)))
    for sub in ["", "ffmpeg", os.path.join("ffmpeg","bin")]:
        p = os.path.join(exe_dir, sub, "ffmpeg.exe")
        if os.path.exists(p):
            os.environ["PATH"] = os.path.dirname(p) + os.pathsep + os.environ.get("PATH","")
            return p
    try:
        if subprocess.run(["ffmpeg","-version"],capture_output=True,timeout=5).returncode == 0:
            return "ffmpeg"
    except Exception:
        pass
    uname = os.environ.get("USERNAME","")
    for path in [
        rf"C:\Users\{uname}\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.0.1-full_build\bin",
        rf"C:\Users\{uname}\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-7.1-full_build\bin",
        r"C:\ProgramData\chocolatey\bin", r"C:\ffmpeg\bin",
        r"C:\Program Files\ffmpeg\bin",   r"C:\Program Files (x86)\ffmpeg\bin",
    ]:
        if os.path.exists(os.path.join(path,"ffmpeg.exe")):
            os.environ["PATH"] = path + os.pathsep + os.environ.get("PATH","")
            return os.path.join(path,"ffmpeg.exe")
    return None

FFMPEG_PATH = find_ffmpeg()

def ffmpeg_ok():
    return FFMPEG_PATH is not None

# ══════════════════════════════════════════════════════════════════════════════
#  UTILS
# ══════════════════════════════════════════════════════════════════════════════
_ANSI = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

def strip_ansi(t):
    return _ANSI.sub("", str(t))

def sanitize(name):
    name = re.sub(r'[\\/*?:"<>|]', "_", name).strip(". ")
    return (name[:60].rstrip() if len(name) > 60 else name) or "download"

def to_mb(b):
    return b / (1024 * 1024)

def get_save_dir():
    d = os.path.join(os.path.expanduser("~"), "Downloads", "FastTube")
    os.makedirs(d, exist_ok=True)
    return d

def get_cookies():
    base = (os.path.dirname(sys.executable) if getattr(sys,"frozen",False)
            else os.path.dirname(os.path.abspath(__file__)))
    p = os.path.join(base, "cookies.txt")
    return p if os.path.exists(p) else None

def base_ydl_opts(pkey="other", ci=0):
    opts = {
        "quiet": True, "no_warnings": True,
        "socket_timeout": SOCK_TO, "retries": MAX_RET,
        "fragment_retries": FRAG_RET, "geo_bypass": True,
        "http_chunk_size": CHUNK,
        "concurrent_fragment_downloads": C_FRAG,
        "continuedl": True,
    }
    if pkey == "youtube":
        _, cl = PLAYER_CLIENTS[ci % len(PLAYER_CLIENTS)]
        opts["extractor_args"] = {"youtube": {"player_client": cl}}
    if pkey in ("instagram","snapchat"):
        opts["http_headers"] = {"User-Agent":
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile Safari/604.1"}
    # Platform-specific options (MX Player etc.)
    opts = get_platform_ydl_opts_extra(pkey, opts)
    if FFMPEG_PATH and FFMPEG_PATH != "ffmpeg":
        opts["ffmpeg_location"] = os.path.dirname(FFMPEG_PATH)
    c = get_cookies()
    if c:
        opts["cookiefile"] = c
    return opts

def quality_to_fmt(q):
    """Convert quality string to yt-dlp format selector."""
    if q == "best":
        return "bestvideo+bestaudio/best"
    if q == "audio_mp3":
        return "bestaudio"
    if q.endswith("p") and q[:-1].isdigit():
        h = int(q[:-1])
        return (f"bestvideo[height<={h}][ext=mp4]+bestaudio[ext=m4a]"
                f"/bestvideo[height<={h}]+bestaudio"
                f"/best[height<={h}]/best")
    return "bestvideo+bestaudio/best"

# ══════════════════════════════════════════════════════════════════════════════
#  MX PLAYER — Dedicated fetch & download helper
#  (placed here so get_cookies / FFMPEG_PATH / strip_ansi are already defined)
# ══════════════════════════════════════════════════════════════════════════════
def mx_fetch_info(url):
    """
    MX Player fetch with 3 fallback strategies.
    Must be called AFTER get_cookies / FFMPEG_PATH / strip_ansi are defined.
    """
    MX_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    MX_HEADERS = {
        "User-Agent":     MX_UA,
        "Referer":        "https://www.mxplayer.in/",
        "Origin":         "https://www.mxplayer.in",
        "Accept":         "*/*",
        "Accept-Language":"en-US,en;q=0.9",
    }

    def _make_opts(extra=None):
        o = {
            "quiet":          True,
            "no_warnings":    True,
            "skip_download":  True,
            "socket_timeout": 60,
            "retries":        5,
            "geo_bypass":     True,
            "http_headers":   MX_HEADERS.copy(),
        }
        ck = get_cookies()
        if ck:
            o["cookiefile"] = ck
        fp = FFMPEG_PATH
        if fp and fp != "ffmpeg":
            o["ffmpeg_location"] = os.path.dirname(fp)
        if extra:
            o.update(extra)
        return o

    strategies = [
        ("native",          _make_opts()),
        ("no-geo-bypass",   _make_opts({"geo_bypass": False})),
        ("force-generic",   _make_opts({"force_generic_extractor": True})),
    ]

    last_err = "Unknown error"
    for name, opts in strategies:
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if info:
                return info
        except Exception as exc:
            last_err = strip_ansi(str(exc))
            continue

    # All strategies failed
    raise Exception(
        f"MX Player fetch failed ({last_err[:120]})\n\n"
        "Tips:\n"
        "• Make sure URL is a public/free video from mxplayer.in\n"
        "• Premium content needs cookies — use Set Cookies button\n"
        "• Some MX Player content is DRM-protected (not downloadable)"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE  (~/Downloads/FastTube/history.db)
# ══════════════════════════════════════════════════════════════════════════════
_DB_PATH = os.path.join(get_save_dir(), "history.db")

def _conn():
    conn = sqlite3.connect(_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            url           TEXT,
            title         TEXT,
            platform      TEXT,
            quality       TEXT,
            file_path     TEXT,
            status        TEXT,
            size_mb       REAL DEFAULT 0,
            duration      TEXT DEFAULT '',
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    conn.commit()
    return conn

def db_insert(url, title, pkey, quality, fpath, status, size_mb=0.0, dur=""):
    try:
        c = _conn()
        c.execute(
            "INSERT INTO history "
            "(url,title,platform,quality,file_path,status,size_mb,duration) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (url, title, pkey, quality, fpath, status, size_mb, dur))
        c.commit(); c.close()
    except Exception:
        pass

def db_all():
    try:
        c = _conn()
        rows = c.execute(
            "SELECT title,platform,quality,status,size_mb,downloaded_at,url "
            "FROM history ORDER BY id DESC LIMIT 500"
        ).fetchall()
        c.close()
        return rows
    except Exception:
        return []

def db_clear():
    try:
        c = _conn(); c.execute("DELETE FROM history"); c.commit(); c.close()
    except Exception:
        pass

def db_check_duplicate(url: str):
    """Return (is_dup, row_or_None) — checks if URL was already downloaded successfully."""
    try:
        c = _conn()
        row = c.execute(
            "SELECT title, quality, downloaded_at, file_path "
            "FROM history WHERE url=? AND status='done' "
            "ORDER BY id DESC LIMIT 1", (url,)
        ).fetchone()
        c.close()
        return (row is not None, row)
    except Exception:
        return (False, None)

def ask_duplicate_popup(parent, url: str, row):
    """
    Show a styled popup when a duplicate URL is detected.
    Returns True  → user chose to download anyway
            False → user cancelled
    Single click on Yes/No works immediately. Enter=Yes, Escape=No.
    """
    title_txt = row[0] if row and row[0] else url[:60]
    quality   = row[1] if row and row[1] else "?"
    dl_at     = row[2] if row and row[2] else "?"
    fpath     = row[3] if row and row[3] else ""
    exists    = bool(fpath and os.path.exists(fpath))

    result = {"go": False}

    # Get the real top-level window so transient/grab work correctly
    root = parent.winfo_toplevel()

    popup = tk.Toplevel(root)
    popup.title("Duplicate Detected")
    popup.configure(bg="#1a0e2e")
    popup.resizable(False, False)
    popup.transient(root)           # attach to parent window
    popup.protocol("WM_DELETE_WINDOW", lambda: None)  # disable X close

    # Build all widgets FIRST, then position & show
    # ── Header ────────────────────────────────────────────────────────────────
    hdr = tk.Frame(popup, bg="#2d1044", pady=14)
    hdr.pack(fill="x")
    tk.Label(hdr, text="⚠️  Duplicate Download Detected",
             font=("Helvetica", 12, "bold"),
             fg="#f0a0ff", bg="#2d1044").pack()

    # ── Info card ─────────────────────────────────────────────────────────────
    card = tk.Frame(popup, bg="#221535", padx=18, pady=12)
    card.pack(fill="x", padx=16, pady=(10, 0))

    def info_row(label, value, vc="#e0d0ff"):
        rf = tk.Frame(card, bg="#221535")
        rf.pack(fill="x", pady=2)
        tk.Label(rf, text=label, font=("Helvetica", 9, "bold"),
                 fg="#9070c0", bg="#221535", width=14, anchor="w").pack(side="left")
        tk.Label(rf, text=value, font=("Helvetica", 9),
                 fg=vc, bg="#221535", anchor="w",
                 wraplength=295, justify="left").pack(side="left", fill="x", expand=True)

    short_title = (title_txt[:52] + "…") if len(title_txt) > 52 else title_txt
    info_row("📹 Title:",      short_title)
    info_row("🎚  Quality:",   quality)
    info_row("📅 Downloaded:", dl_at)
    file_clr = "#50e050" if exists else "#e05050"
    file_txt  = "✅ File exists on disk" if exists else "❌ File not found on disk"
    info_row("📂 File:", file_txt, file_clr)

    # ── Question ──────────────────────────────────────────────────────────────
    tk.Label(popup,
             text="Do you still want to download it again?",
             font=("Helvetica", 10, "bold"),
             fg="#d0b0ff", bg="#1a0e2e").pack(pady=(12, 8))

    # ── Buttons ───────────────────────────────────────────────────────────────
    btn_row = tk.Frame(popup, bg="#1a0e2e")
    btn_row.pack(pady=(0, 16))

    def _yes(_event=None):
        result["go"] = True
        try: popup.grab_release()
        except Exception: pass
        popup.destroy()

    def _no(_event=None):
        result["go"] = False
        try: popup.grab_release()
        except Exception: pass
        popup.destroy()

    yes_btn = tk.Button(btn_row, text="✅  Yes, Download Again",
                        font=("Helvetica", 10, "bold"),
                        bg="#1e6b1e", fg="white", relief="flat", bd=0,
                        padx=20, pady=8, cursor="hand2",
                        activebackground="#28a028", activeforeground="white",
                        command=_yes)
    yes_btn.pack(side="left", padx=(0, 12))

    no_btn = tk.Button(btn_row, text="❌  No, Cancel",
                       font=("Helvetica", 10, "bold"),
                       bg="#6b1e1e", fg="white", relief="flat", bd=0,
                       padx=20, pady=8, cursor="hand2",
                       activebackground="#a02828", activeforeground="white",
                       command=_no)
    no_btn.pack(side="left")

    # Keyboard shortcuts
    popup.bind("<Return>",  _yes)
    popup.bind("<Escape>",  _no)

    # ── Position popup centered on parent, THEN grab ──────────────────────────
    popup.update_idletasks()          # measure real size first
    pw = popup.winfo_reqwidth()
    ph = popup.winfo_reqheight()
    rx = root.winfo_rootx() + (root.winfo_width()  - pw) // 2
    ry = root.winfo_rooty() + (root.winfo_height() - ph) // 2
    popup.geometry(f"{pw}x{ph}+{rx}+{ry}")

    # Fully render before grabbing so first click lands on a button, not air
    popup.deiconify()
    popup.update()          # flush all pending draw events
    popup.lift()
    popup.focus_force()     # bring keyboard focus here
    popup.grab_set()        # NOW grab — window is fully visible

    popup.wait_window()
    return result["go"]

# Ensure DB is created on startup
_conn().close()

# ══════════════════════════════════════════════════════════════════════════════
#  AUTH & SUBSCRIPTION SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
ADMIN_EMAIL  = "tyson170902@gmail.com"
ADMIN_UPI_ID = "dasharshvardhan0-1@oksbi"   # ← apna real UPI ID daalo

# ── Secure config file (password yahan save hota hai, code mein nahi) ────────
_CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".fasttube_config.json")

def _load_config():
    try:
        with open(_CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_config(data):
    try:
        cfg = _load_config()
        cfg.update(data)
        with open(_CONFIG_FILE, "w") as f:
            json.dump(cfg, f)
    except Exception:
        pass

def _get_admin_pw():
    """Get admin password from secure local config file."""
    return _load_config().get("admin_pw", "")

def _set_admin_pw(pw):
    """Save admin password to local config file."""
    _save_config({"admin_pw": pw})
_AUTH_DB    = os.path.join(get_save_dir(), "fasttube_auth.db")

def _auth_conn():
    conn = sqlite3.connect(_AUTH_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            last_login TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_subscription (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            plan_type TEXT DEFAULT 'FREE',
            start_date TEXT,
            expiry_date TEXT,
            daily_count INTEGER DEFAULT 0,
            weekly_count INTEGER DEFAULT 0,
            last_reset TEXT,
            status TEXT DEFAULT 'ACTIVE',
            FOREIGN KEY(user_id) REFERENCES users(id)
        )""")
    conn.commit()
    return conn

_auth_conn().close()


PLANS = {
    "FREE":    {"name":"Free",    "price":0,   "dl_week":1,  "dl_day":0,  "max_res":720,  "batch":False,"meta":False,"compare":False},
    "DAILY":   {"name":"Daily",   "price":51,  "dl_week":3,  "dl_day":3,  "max_res":1080, "batch":False,"meta":False,"compare":False},
    "MONTHLY": {"name":"Monthly", "price":199, "dl_week":0,  "dl_day":-1, "max_res":2160, "batch":True, "meta":True, "compare":False},
    "YEARLY":  {"name":"Yearly",  "price":899, "dl_week":0,  "dl_day":-1, "max_res":2160, "batch":True, "meta":True, "compare":True},
}


# ── Feature keys (used in limits table + usage counters) ──────────────────────


FEATURES = ["batch", "single", "audio", "convert", "subtitles", "metadata", "thumbnail"]

# ── Default per-feature limits per plan (admin can change via panel) ──────────
# -1 = unlimited
_DEFAULT_LIMITS = {
    #            batch  single  audio  convert  subtitles  metadata  thumbnail
    "FREE":    [    1,     1,      1,       1,         1,         1,        1  ],
    "DAILY":   [    3,     3,      3,       3,         3,         3,        3  ],
    "MONTHLY": [   10,    10,     10,      10,        10,        10,       10  ],
    "YEARLY":  [   20,    20,     20,      20,        20,        20,       20  ],
}

def _ensure_limits_table(conn):
    """Create plan_feature_limits, feature_usage, and plan_settings tables if not exist."""
    # Feature usage limits table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS plan_feature_limits (
            plan_type  TEXT NOT NULL,
            feature    TEXT NOT NULL,
            max_uses   INTEGER DEFAULT 1,
            reset_days INTEGER DEFAULT 7,
            PRIMARY KEY (plan_type, feature)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_usage (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            feature    TEXT NOT NULL,
            used_count INTEGER DEFAULT 0,
            period_start TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, feature)
        )""")
    # Plan settings — admin editable: price, description, enabled features
    conn.execute("""
        CREATE TABLE IF NOT EXISTS plan_settings (
            plan_type    TEXT PRIMARY KEY,
            display_name TEXT,
            price        INTEGER DEFAULT 0,
            orig_price   INTEGER DEFAULT 0,
            validity_days INTEGER DEFAULT 0,
            badge        TEXT DEFAULT '',
            enabled      INTEGER DEFAULT 1,
            feat_batch     INTEGER DEFAULT 0,
            feat_single    INTEGER DEFAULT 1,
            feat_audio     INTEGER DEFAULT 0,
            feat_convert   INTEGER DEFAULT 0,
            feat_subtitles INTEGER DEFAULT 0,
            feat_metadata  INTEGER DEFAULT 0,
            feat_thumbnail INTEGER DEFAULT 1,
            feat_4k        INTEGER DEFAULT 0,
            feat_8k        INTEGER DEFAULT 0,
            max_res        INTEGER DEFAULT 720,
            extra_notes    TEXT DEFAULT ''
        )""")
    conn.commit()

    # Seed plan_settings defaults
    _plan_defaults = [
        ("FREE",    "Free",    0,    0,    0,   "",           1, 0,1,0,0,0,0,1,0,0, 720,  "Always free"),
        ("DAILY",   "Daily",   51,   0,    7,   "QUICK",      1, 0,1,1,0,0,0,1,0,0, 1080, "Valid 7 days"),
        ("MONTHLY", "Monthly", 199,  0,    30,  "POPULAR",    1, 1,1,1,1,1,1,1,1,0, 2160, "Valid 30 days"),
        ("YEARLY",  "Yearly",  899,  2350, 365, "BEST VALUE", 1, 1,1,1,1,1,1,1,1,1, 2160, "Save ₹1451 · 365 days"),
    ]
    for row in _plan_defaults:
        conn.execute("""INSERT OR IGNORE INTO plan_settings(
            plan_type,display_name,price,orig_price,validity_days,badge,enabled,
            feat_batch,feat_single,feat_audio,feat_convert,feat_subtitles,feat_metadata,feat_thumbnail,
            feat_4k,feat_8k,max_res,extra_notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
    conn.commit()

    # Seed feature limits — use INSERT OR REPLACE so updated defaults apply
    for plan, limits in _DEFAULT_LIMITS.items():
        reset = 7 if plan in ("FREE","DAILY") else (30 if plan == "MONTHLY" else 365)
        for feat, mx in zip(FEATURES, limits):
            # Only update if the current value matches old wrong default (1 for monthly/yearly)
            # This allows admin-set values to survive while fixing factory-wrong values
            existing = conn.execute(
                "SELECT max_uses FROM plan_feature_limits WHERE plan_type=? AND feature=?",
                (plan, feat)).fetchone()
            if existing is None:
                conn.execute("""
                    INSERT INTO plan_feature_limits(plan_type, feature, max_uses, reset_days)
                    VALUES(?,?,?,?)""", (plan, feat, mx, reset))
            elif existing[0] == 1 and mx > 1:
                # Fix stale default of 1 for plans that should have more
                conn.execute("""
                    UPDATE plan_feature_limits SET max_uses=?, reset_days=?
                    WHERE plan_type=? AND feature=?""", (mx, reset, plan, feat))
    conn.commit()

def get_feature_limit(plan, feature):
    """Returns (max_uses, reset_days) for a plan+feature from DB."""
    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        row = conn.execute(
            "SELECT max_uses, reset_days FROM plan_feature_limits WHERE plan_type=? AND feature=?",
            (plan, feature)).fetchone()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception:
        pass
    idx = FEATURES.index(feature) if feature in FEATURES else 0
    return _DEFAULT_LIMITS.get(plan, _DEFAULT_LIMITS["FREE"])[idx], 7

def get_all_plan_settings():
    """
    Load all plan settings from DB for UpgradeDialog.
    Returns dict: {plan_type: {...}}
    """
    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        rows = conn.execute("""
            SELECT plan_type,display_name,price,orig_price,validity_days,badge,enabled,
                   feat_batch,feat_single,feat_audio,feat_convert,feat_subtitles,
                   feat_metadata,feat_thumbnail,feat_4k,feat_8k,max_res,extra_notes
            FROM plan_settings ORDER BY price ASC""").fetchall()
        # Also load live feature limits from DB
        lim_rows = conn.execute(
            "SELECT plan_type, feature, max_uses, reset_days FROM plan_feature_limits"
        ).fetchall()
        conn.close()
    except Exception:
        rows = []; lim_rows = []

    # Build live limits dict
    live_limits = {}
    for plan, feat, mx, rd in lim_rows:
        live_limits.setdefault(plan, {})[feat] = (mx, rd)

    result = {}
    for row in rows:
        (pt, dname, price, orig, vdays, badge, enabled,
         fb, fs, fa, fconv, fsub, fmeta, fthumb, f4k, f8k, mres, notes) = row

        feat_flags = {
            "batch": bool(fb), "single": bool(fs), "audio": bool(fa),
            "convert": bool(fconv), "subtitles": bool(fsub),
            "metadata": bool(fmeta), "thumbnail": bool(fthumb),
            "4k": bool(f4k), "8k": bool(f8k),
        }

        # Get live max_uses for this plan (from admin-editable limits)
        plan_lims = live_limits.get(pt, {})
        batch_mx  = plan_lims.get("batch",  (1,7))[0]
        period    = plan_lims.get("batch",  (1,7))[1]

        # Build visible feature list
        feats = []

        # Usage line — show actual live limits from DB
        if batch_mx == -1:
            feats.append("Unlimited uses")
        else:
            feats.append(f"{batch_mx} uses per feature / {period} days")

        feat_map = [
            ("batch",     "Batch Download"),
            ("single",    "Single Download"),
            ("audio",     "Audio Extract"),
            ("convert",   "Format Convert"),
            ("subtitles", "Subtitles"),
            ("metadata",  "Metadata Studio"),
            ("thumbnail", "Thumbnail DL"),
            ("4k",        "4K Quality"),
            ("8k",        "8K Quality"),
        ]
        for fkey, flabel in feat_map:
            if feat_flags.get(fkey):
                feats.append(f"✅ {flabel}")
            else:
                feats.append(f"❌ {flabel}")

        if notes:
            feats.append(notes)

        # Build validity string
        if vdays == 0:
            sub = "Always free"
        elif vdays == 7:
            sub = "Valid 7 days"
        elif vdays == 30:
            sub = "Valid 30 days"
        elif vdays == 365:
            sub = f"Save ₹{orig-price}  •  365 days" if orig > price else "Valid 365 days"
        else:
            sub = f"Valid {vdays} days"

        result[pt] = {
            "key": pt,
            "name": dname or pt,
            "price": f"₹{price}",
            "orig": f"₹{orig}" if orig > price else "",
            "sub": sub,
            "badge": badge or "",
            "enabled": bool(enabled),
            "feat_flags": feat_flags,
            "feats": feats,
            "max_res": mres,
            "validity_days": vdays,
            "raw_price": price,
        }
    return result

def sub_check_feature(feature):
    """
    Check if user can use a feature:
    1. Is feature enabled for this plan in plan_settings?
    2. Has usage limit been reached?
    Returns (ok:bool, reason:str, used:int, limit:int)
    """
    _refresh_sub()
    plan = _S.get("plan", "FREE")
    uid  = _S.get("uid")
    if not uid:
        return False, "Not logged in.", 0, 0

    # ── Check if feature is enabled for this plan ──────────────────────
    feat_col_map = {
        "batch":"feat_batch", "single":"feat_single", "audio":"feat_audio",
        "convert":"feat_convert", "subtitles":"feat_subtitles",
        "metadata":"feat_metadata", "thumbnail":"feat_thumbnail",
    }
    col = feat_col_map.get(feature)
    if col:
        try:
            conn = _auth_conn()
            _ensure_limits_table(conn)
            row = conn.execute(
                f"SELECT {col} FROM plan_settings WHERE plan_type=?", (plan,)
            ).fetchone()
            conn.close()
            if row and not bool(row[0]):
                plan_name = PLANS.get(plan, {}).get("name", plan)
                return False, (
                    f"⛔ {feature.title()} is not available in {plan_name} plan.\n\n"
                    f"Upgrade your plan to unlock this feature."
                ), 0, 0
        except Exception:
            pass  # fail open if DB error

    # ── Check usage limit ──────────────────────────────────────────────
    max_uses, reset_days = get_feature_limit(plan, feature)
    if max_uses == -1:
        return True, "", -1, -1

    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        row = conn.execute(
            "SELECT used_count, period_start FROM feature_usage WHERE user_id=? AND feature=?",
            (uid, feature)).fetchone()
        now = datetime.now()

        if row:
            used, ps = row
            try:
                period_start = datetime.fromisoformat(ps)
                if (now - period_start).days >= reset_days:
                    conn.execute(
                        "UPDATE feature_usage SET used_count=0, period_start=? WHERE user_id=? AND feature=?",
                        (now.isoformat(), uid, feature))
                    conn.commit()
                    used = 0
            except Exception:
                used = 0
        else:
            used = 0
        conn.close()

        if used >= max_uses:
            plan_name = PLANS.get(plan, {}).get("name", plan)
            return False, (
                f"⛔ {feature.title()} limit reached!\n"
                f"{plan_name} plan: {max_uses} uses per {reset_days} days\n"
                f"Used: {used}/{max_uses}\n\n"
                f"Upgrade your plan to get more uses."
            ), used, max_uses

        return True, "", used, max_uses
    except Exception:
        return True, "", 0, 0  # fail open

def sub_record_feature(feature):
    """Increment feature usage counter after successful use."""
    uid  = _S.get("uid")
    plan = _S.get("plan", "FREE")
    if not uid: return
    max_uses, _ = get_feature_limit(plan, feature)
    if max_uses == -1: return  # unlimited — don't track
    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        conn.execute("""
            INSERT INTO feature_usage(user_id, feature, used_count, period_start)
            VALUES(?,?,1,datetime('now'))
            ON CONFLICT(user_id, feature) DO UPDATE
            SET used_count = used_count + 1""",
            (uid, feature))
        conn.commit(); conn.close()
        _refresh_sub()
    except Exception:
        pass

def sub_reset_feature_usage(uid=None, feature=None):
    """
    Reset usage counters.
    uid=None → reset ALL users; feature=None → reset all features.
    Admin uses this to fix stuck users.
    """
    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        if uid and feature:
            conn.execute("DELETE FROM feature_usage WHERE user_id=? AND feature=?", (uid, feature))
        elif uid:
            conn.execute("DELETE FROM feature_usage WHERE user_id=?", (uid,))
        elif feature:
            conn.execute("DELETE FROM feature_usage WHERE feature=?", (feature,))
        else:
            conn.execute("DELETE FROM feature_usage")
        conn.commit(); conn.close()
        return True
    except Exception:
        return False

def sub_get_usage_summary(uid):
    """Returns list of (feature, used_count, max_uses, reset_days, period_start) for a user."""
    try:
        conn = _auth_conn()
        _ensure_limits_table(conn)
        plan = _S.get("plan", "FREE")
        rows = conn.execute(
            "SELECT feature, used_count, period_start FROM feature_usage WHERE user_id=?",
            (uid,)).fetchall()
        conn.close()
        result = []
        for feat, used, ps in rows:
            mx, rd = get_feature_limit(plan, feat)
            result.append((feat, used, mx, rd, ps))
        return result
    except Exception:
        return []


def _hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

# ── Live session dict ─────────────────────────────────────────────────────────
_S = {"uid":None,"name":"","email":"","plan":"FREE",
      "expiry":None,"daily":0,"weekly":0,"reset":None}

def _sess_file():
    return os.path.join(get_save_dir(), ".ftsession")

def _save_sess():
    try:
        with open(_sess_file(),"w") as f:
            json.dump({"uid":_S["uid"]}, f)
    except Exception:
        pass

def _clear_sess():
    try: os.remove(_sess_file())
    except Exception: pass
    _S.update({"uid":None,"name":"","email":"","plan":"FREE",
               "expiry":None,"daily":0,"weekly":0,"reset":None})

def _load_sess():
    """Auto-login from saved session. Returns True if session restored."""
    if not os.path.exists(_sess_file()):
        return False
    try:
        with open(_sess_file(),"r") as f:
            data = json.load(f)
        uid = data.get("uid")
        if not uid: return False
        conn = _auth_conn()
        row  = conn.execute(
            "SELECT id,name,email FROM users WHERE id=?", (uid,)).fetchone()
        conn.close()
        if not row: return False
        _S["uid"]   = row[0]
        _S["name"]  = row[1]
        _S["email"] = row[2]
        _refresh_sub()
        return True
    except Exception:
        return False

def _refresh_sub():
    uid = _S["uid"]
    if not uid: return
    conn = _auth_conn()
    row  = conn.execute(
        "SELECT plan_type,expiry_date,daily_count,weekly_count,last_reset,status "
        "FROM user_subscription WHERE user_id=? ORDER BY id DESC LIMIT 1",
        (uid,)).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO user_subscription(user_id,plan_type,start_date,status) "
            "VALUES(?,?,?,?)", (uid,"FREE",datetime.now().isoformat(),"ACTIVE"))
        conn.commit()
        _S["plan"]="FREE"; conn.close(); return

    plan,expiry,dc,wc,lr,status = row

    # ── Auto-expiry ──────────────────────────────────────────────────────────
    if expiry and status=="ACTIVE":
        try:
            if datetime.now() > datetime.fromisoformat(expiry):
                conn.execute(
                    "UPDATE user_subscription SET status='EXPIRED',plan_type='FREE' "
                    "WHERE user_id=?",(uid,))
                conn.commit()
                plan="FREE"; expiry=None; dc=0; wc=0
        except Exception: pass

    # ── Daily reset ──────────────────────────────────────────────────────────
    now = datetime.now()
    if plan=="DAILY" and lr:
        try:
            if now - datetime.fromisoformat(lr) > timedelta(days=7):
                conn.execute(
                    "UPDATE user_subscription SET daily_count=0,last_reset=? "
                    "WHERE user_id=?",(now.isoformat(),uid))
                conn.commit(); dc=0
        except Exception: pass

    # ── Weekly reset ─────────────────────────────────────────────────────────
    if plan == "FREE" and lr:
        try:
            if now - datetime.fromisoformat(lr) >= timedelta(days=7):
                conn.execute(
                    "UPDATE user_subscription "
                    "SET weekly_count=0, last_reset=? "
                    "WHERE user_id=?",
                    (now.isoformat(), uid))
                conn.commit()
                wc = 0
        except Exception:
            pass

    conn.close()
    _S.update({"plan":plan,"expiry":expiry,"daily":dc or 0,
               "weekly":wc or 0,"reset":lr})

def sub_can_dl(quality="720p"):
    """Returns (ok, reason). Call before every download."""
    _refresh_sub()
    plan = _S["plan"]
    pi   = PLANS.get(plan, PLANS["FREE"])
    # Quality gate
    if quality:
        try:
            h = int(re.search(r'(\d+)', quality).group(1))
            if h > pi["max_res"]:
                return False, (f"{quality} requires Monthly/Yearly plan.\n"
                               "Please upgrade to continue.")
        except Exception: pass
    # Count gate
    if plan=="FREE" and _S["weekly"] >= 1:
        return False, "Free plan: 1 video/week limit reached.\nUpgrade to continue."
    if plan=="DAILY" and _S["daily"] >= 3:
        return False, "Daily plan: 3 videos/week limit reached.\nUpgrade to continue."
    return True, ""

def sub_feature_ok(feat):
    """feat: 'batch'|'4k'|'metadata'|'compare'. Returns (ok, msg)."""
    _refresh_sub()
    pi = PLANS.get(_S["plan"], PLANS["FREE"])
    msgs = {
        "batch":   ("Batch download requires Monthly or Yearly plan.", pi["batch"]),
        "4k":      ("4K requires Monthly or Yearly plan.",             pi["max_res"]>=2160),
        "metadata":("Metadata Studio requires Monthly or Yearly plan.",pi["meta"]),
        "compare": ("Compare Export requires Yearly plan.",            pi["compare"]),
    }
    msg, ok = msgs.get(feat, ("", True))
    return ok, ("" if ok else msg)

def sub_record_dl():
    """Increment counter after successful download."""
    uid  = _S["uid"]
    plan = _S["plan"]
    if not uid: return
    now  = datetime.now().isoformat()
    conn = _auth_conn()
    if plan == "FREE":
        # Always set last_reset on first download so weekly window is anchored
        conn.execute(
            "UPDATE user_subscription "
            "SET weekly_count = weekly_count + 1, "
            "last_reset = ? "
            "WHERE user_id = ?",
            (now, uid))
    elif plan == "DAILY":
        conn.execute(
            "UPDATE user_subscription "
            "SET daily_count = daily_count + 1, "
            "last_reset = COALESCE(last_reset, ?) "
            "WHERE user_id = ?",
            (now, uid))
    conn.commit(); conn.close(); _refresh_sub()

def sub_activate(plan_key):
    uid = _S["uid"]
    if not uid: return
    now    = datetime.now()
    days   = {"FREE":None,"DAILY":7,"MONTHLY":30,"YEARLY":365}
    d      = days.get(plan_key)
    expiry = (now+timedelta(days=d)).isoformat() if d else None
    conn   = _auth_conn()
    conn.execute("DELETE FROM user_subscription WHERE user_id=?",(uid,))
    conn.execute(
        "INSERT INTO user_subscription"
        "(user_id,plan_type,start_date,expiry_date,daily_count,"
        "weekly_count,last_reset,status) VALUES(?,?,?,?,0,0,?,?)",
        (uid,plan_key,now.isoformat(),expiry,now.isoformat(),"ACTIVE"))
    conn.commit(); conn.close(); _refresh_sub()
    threading.Thread(target=_notify_admin, args=(plan_key,), daemon=True).start()

# ── Gmail App Password — set karo ────────────────────────────────────────────
# myaccount.google.com → Security → 2-Step ON → App Passwords → Generate
GMAIL_APP_PW = "xzza ngtw dzfq ukbw"

def _send_email(to_addr, subject, body):
    """Send email via Gmail SMTP. Best-effort — never crashes app."""
    try:
        msg = MIMEMultipart("alternative")
        msg["From"]    = ADMIN_EMAIL
        msg["To"]      = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as s:
            s.login(ADMIN_EMAIL, GMAIL_APP_PW)
            s.send_message(msg)
    except Exception:
        pass

def _notify_admin(plan_key):
    """Email admin + user when plan activated."""
    pi    = PLANS.get(plan_key, {})
    name  = _S["name"]
    email = _S["email"]
    price = pi.get("price", 0)
    now   = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── Admin notification ───────────────────────────────────────────────────
    admin_body = (
        f"New subscription activated!\n"
        f"{'='*40}\n"
        f"Name   : {name}\n"
        f"Email  : {email}\n"
        f"Plan   : {plan_key}\n"
        f"Price  : ₹{price}\n"
        f"Time   : {now}\n"
        f"{'='*40}\n"
        f"[FastTube Pro Admin Alert]"
    )
    # Direct call — already inside _notify_admin thread
    _send_email(ADMIN_EMAIL,
                f"[FastTube Pro] New {plan_key} — {name}",
                admin_body)

    # ── User welcome / confirmation email ────────────────────────────────────
    if plan_key == "FREE":
        user_subj = "Welcome to FastTube Pro! 🎉"
        user_body = (
            f"Hi {name},\n\n"
            f"Welcome to FastTube Pro!\n\n"
            f"Your free account is ready.\n"
            f"Plan   : Free\n"
            f"Limit  : 1 video per week, max 720p\n\n"
            f"Upgrade anytime for more downloads and higher quality.\n\n"
            f"Happy downloading!\n"
            f"— FastTube Pro Team"
        )
    else:
        exp = _S.get("expiry","")
        exp_str = exp[:10] if exp else "N/A"
        user_subj = f"FastTube Pro — {plan_key.title()} Plan Activated ✅"
        user_body = (
            f"Hi {name},\n\n"
            f"Your {plan_key.title()} Plan has been activated successfully!\n\n"
            f"Plan   : {plan_key}\n"
            f"Price  : ₹{price}\n"
            f"Expiry : {exp_str}\n"
            f"Time   : {now}\n\n"
            f"Enjoy unlimited downloads and premium features!\n\n"
            f"— FastTube Pro Team"
        )
    # Direct call — already inside _notify_admin thread
    _send_email(email, user_subj, user_body)

# Temp/disposable email domains — block karo
_BLOCKED_DOMAINS = {
    # ── Guerrilla Mail family ─────────────────────────────────────────────────
    "guerrillamail.com","guerrillamail.net","guerrillamail.org",
    "guerrillamail.biz","guerrillamail.de","guerrillamail.info",
    "guerrillamailblock.com","sharklasers.com","grr.la","spam4.me",
    "guerrillamailblock.com","yopmail.com","yopmail.fr","cool.fr.nf",
    "jetable.fr.nf","nospam.ze.tc","nomail.xl.cx","mega.zik.dj",
    # ── Mailinator family ────────────────────────────────────────────────────
    "mailinator.com","mailinator2.com","mailinator.net","mailinator.org",
    "mailinater.com","suremail.info","spamherelots.com","binkmail.com",
    "safetymail.info","spam4.me","inoutmail.de","inoutmail.eu",
    "inoutmail.info","inoutmail.net",
    # ── 10 Minute Mail family ────────────────────────────────────────────────
    "10minutemail.com","10minutemail.net","10minutemail.org",
    "10minutemail.de","10minutemail.co.uk","10minutemail.ru",
    "10minemail.com","10mail.org","minutemailbox.com",
    # ── Trash Mail family ────────────────────────────────────────────────────
    "trashmail.com","trashmail.me","trashmail.net","trashmail.at",
    "trashmail.io","trashmail.org","trashmail.xyz","trashemails.de",
    "mt2015.com","mt2016.com","mt2017.com","dispostable.com",
    # ── Temp Mail / Disposable generic ──────────────────────────────────────
    "tempmail.com","tempmail.net","tempmail.org","tempmail.de",
    "temp-mail.org","temp-mail.io","temp-mail.ru","temporarymail.com",
    "tempr.email","tempinbox.com","tempinbox.co.uk","throwam.com",
    "throwaway.email","throwam.com","throwam.net","thrma.com",
    "fakeinbox.com","fakeinbox.net","fadingemail.com",
    "discard.email","discardmail.com","discardmail.de",
    # ── Ostahie / Spamgourmet / Nada ────────────────────────────────────────
    "ostahie.com","spamgourmet.com","spamgourmet.net","spamgourmet.org",
    "maildrop.cc","maildrop.net","getnada.com","nada.ltd","mailnull.com",
    "spamfree24.org","spamfree.eu","spamoff.de","nobulk.com",
    # ── Fake persona domains (Mailgen) ──────────────────────────────────────
    "armyspy.com","cuvox.de","dayrep.com","einrot.com","fleckens.hu",
    "superrito.com","teleworm.us","jourrapide.com","rhyta.com","gustr.com",
    "sanstr.com","antichef.com","antichef.net","antireg.com","antireg.ru",
    "antispam.de","antispam24.de","antispammail.de",
    # ── Mailnesia / Spamevader ───────────────────────────────────────────────
    "mailnesia.com","mailnull.com","spamevader.com","spam.la",
    "spam.su","spaml.com","spaml.de","spamoff.de","spamthisplease.com",
    # ── Wegwerfemail / German disposables ────────────────────────────────────
    "wegwerfemail.de","wegwerfemail.net","wegwerfemail.org",
    "emailondeck.com","e4ward.com","filzmail.com","filzmail.de",
    # ── Mailexpire / Short-lived ─────────────────────────────────────────────
    "mailexpire.com","mailforspam.com","mailfree.ga","mailfreeonline.com",
    "mailfs.com","mailguard.me","mailhazard.com","mailimate.com",
    "mailkept.com","mailme.gq","mailme.ir","mailme24.com","mailmetrash.com",
    "mailmoat.com","mailms.com","mailnew.com","mailnull.com",
    "mailpoof.com","mailproof.com","mailquack.com","mailrock.biz",
    "mailsac.com","mailscrap.com","mailshell.com","mailsiphon.com",
    "mailslapping.com","mailslite.com","mailsnull.com","mailsoul.com",
    "mailspam.me","mailspam.xyz","mailsucker.net","mailtothis.com",
    "mailzilla.com","mailzilla.org",
    # ── Throwam / Jetable ───────────────────────────────────────────────────
    "jetable.com","jetable.fr","jetable.net","jetable.org","jetable.pp.ua",
    "notsharingmy.info","chacuo.net","objectmail.com","obobbo.com",
    # ── Spam box services ────────────────────────────────────────────────────
    "spambox.us","spambox.info","spambox.irishspringrealty.com",
    "spambox.org","spamcero.com","spamcon.org","spamcorptastic.com",
    "spamcowboy.com","spamcowboy.net","spamcowboy.org","spamday.com",
    "spamdecoy.net","spamex.com","spamfighter.cf","spamgoes.in",
    "spamgourmet.com","spamgourmet.net","spamgourmet.org",
    # ── Inboxalias / Emailna ────────────────────────────────────────────────
    "inboxalias.com","emailna.com","emailnax.com","emailo.pro",
    "emailondeck.com","emailsensei.com","emailtemporario.com.br",
    "emailthe.net","emailtmp.com","emailwarden.com","emailx.at.hm",
    "emailxfer.com","emailz.cf","emailz.ga","emailz.gq","emailz.ml",
    # ── Popular Indian temp mail services ───────────────────────────────────
    "tempmail.in","tempmail.co.in","disposablemail.com","throwmail.com",
    "tempinbox.in","mail-temp.com","mohmal.com","dispostable.com",
    # ── Misc known disposable ────────────────────────────────────────────────
    "haltospam.com","hatespam.org","herp.in","hidebox.org",
    "hidemail.de","hidemail.pro","hidzz.com","HighWayMail.me",
    "hortongroup.com","hostguru.info","hotpop.com","hulapla.de",
    "ieatspam.eu","ieatspam.info","ieh-mail.de","ihateyoualot.info",
    "iheartspam.org","imails.info","inboxclean.com","inboxclean.org",
    "incognitomail.com","incognitomail.net","incognitomail.org",
    "inemail.de","instant-mail.de","ip6.li","ipoo.org",
    "irish2me.com","iwi.net","jetable.com","jnxjn.com","junk1.tk",
    "junkmail.ga","junkmail.gq","kasmail.com","kaspop.com",
    "keepmymail.com","killmail.com","killmail.net","klassmaster.com",
    "klzlk.com","koszmail.pl","kurzepost.de","letthemeatspam.com",
    "lhsdv.com","lifebyfood.com","link2mail.net","litedrop.com",
    "lol.ovpn.to","lookugly.com","lortemail.dk","lr78.com","lroid.com",
}

# Keywords in domain that strongly indicate temp/disposable email
_BLOCKED_KEYWORDS = [
    "temp", "trash", "spam", "fake", "dispos", "throwaway",
    "junk", "guerrilla", "mailinator", "yopmail", "10minute",
    "burner", "noreply", "nomail", "getairmail", "filzmail",
    "wegwerf", "inboxkitten", "sharklaser", "throwam",
    "maildrop", "mailnull", "spamfree", "spambox", "spamoff",
    "trashmail", "discard", "jetable", "wegwerf", "mailbucket",
    "inboxalias", "emailondeck", "anonymousemail",
]

# Allowed real providers — whitelist (never block these)
_ALLOWED_DOMAINS = {
    "gmail.com","yahoo.com","yahoo.in","yahoo.co.in","outlook.com",
    "hotmail.com","live.com","icloud.com","me.com","mac.com",
    "rediffmail.com","protonmail.com","proton.me","tutanota.com",
    "zoho.com","aol.com","gmx.com","gmx.net","mail.com",
    "yandex.com","yandex.ru","fastmail.com","pm.me",
}

def _is_temp_email(email):
    """
    Returns True if email is from a known temp/disposable service.
    Checks:
    1. Domain in explicit blocklist
    2. Domain contains suspicious keywords
    3. Domain NOT in known-good whitelist (for unknown domains)
    """
    try:
        em     = email.strip().lower()
        domain = em.split("@")[1]

        # Whitelisted real providers → always allow
        if domain in _ALLOWED_DOMAINS:
            return False

        # Explicit blocklist
        if domain in _BLOCKED_DOMAINS:
            return True

        # Keyword check in domain
        for kw in _BLOCKED_KEYWORDS:
            if kw in domain:
                return True

        # Unknown domain → allow (don't over-block)
        return False
    except Exception:
        return False

def auth_signup(name, email, password):
    if not name.strip() or not email.strip() or not password:
        return False, "Saare fields bharo."
    if "@" not in email or "." not in email:
        return False, "Valid email daalo."
    if _is_temp_email(email):
        return False, (
            "❌ Temporary/disposable email allowed nahi hai!\n\n"
            "Real email use karo jaise:\n"
            "  • Gmail  (yourname@gmail.com)\n"
            "  • Yahoo  (yourname@yahoo.com)\n"
            "  • Outlook (yourname@outlook.com)\n"
            "  • Rediff  (yourname@rediffmail.com)"
        )
    if len(password) < 6:
        return False, "Password minimum 6 characters."
    conn = _auth_conn()
    try:
        conn.execute(
            "INSERT INTO users(name,email,password_hash) VALUES(?,?,?)",
            (name.strip(), email.strip().lower(), _hash_pw(password)))
        conn.commit()
        uid = conn.execute(
            "SELECT id FROM users WHERE email=?",
            (email.strip().lower(),)).fetchone()[0]
        conn.close()
        _S.update({"uid": uid, "name": name.strip(),
                   "email": email.strip().lower()})
        sub_activate("FREE")   # also triggers welcome email via _notify_admin
        _save_sess()
        return True, "Account created!"
    except sqlite3.IntegrityError:
        conn.close(); return False, "Email already registered."
    except Exception as e:
        conn.close(); return False, str(e)

def auth_login(email, password, remember=True):
    conn = _auth_conn()
    row  = conn.execute(
        "SELECT id,name FROM users WHERE email=? AND password_hash=?",
        (email.strip().lower(), _hash_pw(password))).fetchone()
    if not row:
        conn.close(); return False, "Email ya password galat hai."
    uid, name = row
    conn.execute("UPDATE users SET last_login=? WHERE id=?",
                 (datetime.now().isoformat(), uid))
    conn.commit(); conn.close()
    _S.update({"uid": uid, "name": name,
               "email": email.strip().lower()})
    _refresh_sub()
    if remember:
        _save_sess()   # auto-login next time
    return True, "Login successful!"


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH WINDOW  (Login / Signup — opens before main app)
# ══════════════════════════════════════════════════════════════════════════════
class AuthWindow(tk.Toplevel):
    """
    Login / Create Account window.
    Two completely separate frames — no pack(before=) trick.
    Includes: Remember Me, Forgot Password (OTP), Show/Hide password,
              Confirm Password on signup, Enter key support.
    """
    def __init__(self, master, on_success):
        super().__init__(master)
        self._on_success = on_success
        self._remember   = tk.BooleanVar(value=True)
        self._mode       = "login"
        self.title("FastTube Pro — Login")
        self.geometry("460x560")
        self.resizable(False, False)
        self.configure(bg=BG)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._close)
        self._build()
        self._center()

    def _center(self):
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _close(self):
        self.destroy()
        if not _S["uid"]:
            self.master.destroy()

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _field(self, parent, label, show=""):
        """Create a labeled entry. Returns (frame, StringVar)."""
        f  = tk.Frame(parent, bg=BG)
        f.pack(fill="x", pady=(0, 10))
        tk.Label(f, text=label, font=("Helvetica", 8, "bold"),
                 fg=MUTED, bg=BG).pack(anchor="w")
        row = tk.Frame(f, bg=BG_INPUT)
        row.pack(fill="x", pady=(3, 0))
        var = tk.StringVar()
        ent = tk.Entry(row, textvariable=var, show=show,
                       font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
                       insertbackground=WHITE, relief="flat", bd=0)
        ent.pack(side="left", fill="x", expand=True, ipady=8, padx=(8, 0))
        if show:
            _vis = [False]
            def _toggle(v=var, e=ent, s=[False]):
                s[0] = not s[0]
                e.config(show="" if s[0] else "●")
                eye.config(fg=WHITE if s[0] else MUTED)
            eye = tk.Button(row, text="👁", font=("Helvetica", 9),
                            bg=BG_INPUT, fg=MUTED, relief="flat", bd=0,
                            cursor="hand2", command=_toggle)
            eye.pack(side="right", padx=6)
        return f, var

    def _btn(self, parent, text, cmd, bg=ACCENT, fg=WHITE):
        return tk.Button(parent, text=text,
                         font=("Helvetica", 11, "bold"),
                         bg=bg, fg=fg, relief="flat", bd=0,
                         pady=11, cursor="hand2", command=cmd)

    # ── Master build ──────────────────────────────────────────────────────────
    def _build(self):
        # ── Logo ─────────────────────────────────────────────────────────────
        tk.Label(self, text="⚡ FastTube Pro",
                 font=("Helvetica", 22, "bold"),
                 fg=ACCENT, bg=BG).pack(pady=(24, 2))
        tk.Label(self, text="Universal Media Downloader",
                 font=("Helvetica", 9), fg=MUTED, bg=BG).pack()
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x", pady=12)

        # ── Tab switcher ─────────────────────────────────────────────────────
        tog = tk.Frame(self, bg=BG_INPUT, padx=3, pady=3)
        tog.pack(padx=30, fill="x")
        self._t_login = tk.Button(tog, text="Login",
            font=("Helvetica", 10, "bold"), bg=ACCENT, fg=WHITE,
            relief="flat", bd=0, padx=16, pady=7, cursor="hand2",
            command=lambda: self._sw("login"))
        self._t_login.pack(side="left", fill="x", expand=True)
        self._t_signup = tk.Button(tog, text="Create Account",
            font=("Helvetica", 10, "bold"), bg=BG_INPUT, fg=MUTED,
            relief="flat", bd=0, padx=16, pady=7, cursor="hand2",
            command=lambda: self._sw("signup"))
        self._t_signup.pack(side="left", fill="x", expand=True)

        # ── Container — both forms live here, one shown at a time ────────────
        self._container = tk.Frame(self, bg=BG)
        self._container.pack(fill="both", expand=True, padx=30, pady=6)

        self._build_login_form()
        self._build_signup_form()

        # Footer
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")
        foot = tk.Frame(self, bg=BG)
        foot.pack(fill="x", pady=7)
        tk.Label(foot,
                 text="🔒  Secure  ·  Free account  ·  No credit card",
                 font=("Helvetica", 7), fg=DIM, bg=BG).pack(side="left", padx=14)
        admin_lbl = tk.Label(foot, text="Admin",
                 font=("Helvetica", 7), fg="#333333", bg=BG,
                 cursor="hand2")
        admin_lbl.pack(side="right", padx=14)
        admin_lbl.bind("<Button-1>", lambda e: AdminPanel(self))

        self._sw("login")
        self.bind("<Return>", lambda e: self._submit())

    # ── LOGIN FORM ────────────────────────────────────────────────────────────
    def _build_login_form(self):
        self._login_frm = tk.Frame(self._container, bg=BG)

        _, self._l_email = self._field(self._login_frm, "Email Address")
        self._l_email.set("")  # placeholder clear

        # Password row with Forgot Password link
        pw_hdr = tk.Frame(self._login_frm, bg=BG)
        pw_hdr.pack(fill="x")
        tk.Label(pw_hdr, text="Password",
                 font=("Helvetica", 8, "bold"), fg=MUTED, bg=BG).pack(side="left")
        fp_lbl = tk.Label(pw_hdr, text="Forgot Password?",
                 font=("Helvetica", 8), fg=BLUE, bg=BG,
                 cursor="hand2")
        fp_lbl.pack(side="right")
        fp_lbl.bind("<Button-1>", lambda e: self._forgot_pw())

        pw_row = tk.Frame(self._login_frm, bg=BG_INPUT)
        pw_row.pack(fill="x", pady=(3, 10))
        self._l_pw = tk.StringVar()
        self._l_pw_ent = tk.Entry(pw_row, textvariable=self._l_pw, show="●",
            font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
            insertbackground=WHITE, relief="flat", bd=0)
        self._l_pw_ent.pack(side="left", fill="x", expand=True,
                             ipady=8, padx=(8, 0))
        def _toggle_lpw():
            s = self._l_pw_ent.cget("show")
            self._l_pw_ent.config(show="" if s else "●")
            _eye_l.config(fg=WHITE if s else MUTED)
        _eye_l = tk.Button(pw_row, text="👁", font=("Helvetica", 9),
                           bg=BG_INPUT, fg=MUTED, relief="flat", bd=0,
                           cursor="hand2", command=_toggle_lpw)
        _eye_l.pack(side="right", padx=6)

        # Remember Me
        tk.Checkbutton(self._login_frm, text="  Remember Me",
                       variable=self._remember,
                       font=("Helvetica", 8), fg=MUTED, bg=BG,
                       activebackground=BG, activeforeground=WHITE,
                       selectcolor=BG_INPUT, relief="flat", bd=0,
                       cursor="hand2").pack(anchor="w", pady=(0, 4))

        self._l_err = tk.Label(self._login_frm, text="",
                                font=("Helvetica", 8), fg=ACCENT, bg=BG,
                                wraplength=380)
        self._l_err.pack()

        self._l_btn = self._btn(self._login_frm, "Login", self._do_login)
        self._l_btn.pack(fill="x", pady=(8, 0))

    # ── SIGNUP FORM ───────────────────────────────────────────────────────────
    def _build_signup_form(self):
        self._signup_frm = tk.Frame(self._container, bg=BG)

        _, self._s_name    = self._field(self._signup_frm, "Full Name")
        _, self._s_email   = self._field(self._signup_frm, "Email Address")

        # Hint below email
        tk.Label(self._signup_frm,
                 text="  ✓ Use real email: Gmail / Yahoo / Outlook / Rediffmail",
                 font=("Helvetica", 7), fg="#555555", bg=BG,
                 anchor="w").pack(fill="x", pady=(0, 6))

        _, self._s_pw      = self._field(self._signup_frm, "Password  (min 6 chars)", show="●")
        _, self._s_cpw     = self._field(self._signup_frm, "Confirm Password", show="●")

        self._s_err = tk.Label(self._signup_frm, text="",
                                font=("Helvetica", 9), fg=ACCENT, bg=BG,
                                wraplength=380, justify="left")
        self._s_err.pack(pady=(4,0), anchor="w")

        self._s_btn = self._btn(self._signup_frm,
                                 "Create Account  →", self._do_signup)
        self._s_btn.pack(fill="x", pady=(6, 0))

        tk.Label(self._signup_frm,
                 text="Already have an account?",
                 font=("Helvetica", 8), fg=MUTED, bg=BG
                 ).pack(pady=(8, 0))
        login_lbl = tk.Label(self._signup_frm, text="Login here",
                 font=("Helvetica", 8, "bold"), fg=BLUE, bg=BG,
                 cursor="hand2")
        login_lbl.pack()
        login_lbl.bind("<Button-1>", lambda e: self._sw("login"))

    # ── Switch ────────────────────────────────────────────────────────────────
    def _sw(self, mode):
        self._mode = mode
        # Clear errors
        self._l_err.config(text="")
        self._s_err.config(text="")

        if mode == "login":
            self._signup_frm.pack_forget()
            self._login_frm.pack(fill="both", expand=True)
            self._t_login.config(bg=ACCENT, fg=WHITE)
            self._t_signup.config(bg=BG_INPUT, fg=MUTED)
            self.title("FastTube Pro — Login")
            self.geometry("460x490")
        else:
            self._login_frm.pack_forget()
            self._signup_frm.pack(fill="both", expand=True)
            self._t_signup.config(bg=ACCENT, fg=WHITE)
            self._t_login.config(bg=BG_INPUT, fg=MUTED)
            self.title("FastTube Pro — Create Account")
            self.geometry("460x600")

        self._center()

    # ── Submit router ─────────────────────────────────────────────────────────
    def _submit(self):
        if self._mode == "login":
            self._do_login()
        else:
            self._do_signup()

    # ── Login logic ───────────────────────────────────────────────────────────
    def _do_login(self):
        em = self._l_email.get().strip()
        pw = self._l_pw.get()
        if not em or not pw:
            self._l_err.config(text="⚠  Email aur password daalo."); return
        self._l_btn.config(state="disabled", text="Logging in…")
        ok, msg = auth_login(em, pw, remember=self._remember.get())
        self._l_btn.config(state="normal", text="Login")
        if ok:
            self.destroy()
            self._on_success()
        else:
            self._l_err.config(text=f"⚠  {msg}")

    # ── Signup logic ──────────────────────────────────────────────────────────
    def _do_signup(self):
        name = self._s_name.get().strip()
        em   = self._s_email.get().strip()
        pw   = self._s_pw.get()
        cpw  = self._s_cpw.get()

        # Validation
        if not name:
            self._s_err.config(text="⚠  Full Name daalo."); return
        if not em or "@" not in em or "." not in em:
            self._s_err.config(text="⚠  Valid email daalo."); return
        if len(pw) < 6:
            self._s_err.config(text="⚠  Password minimum 6 characters."); return
        if pw != cpw:
            self._s_err.config(text="⚠  Passwords match nahi kar rahe."); return

        self._s_btn.config(state="disabled", text="Creating account…")
        ok, msg = auth_signup(name, em, pw)
        self._s_btn.config(state="normal", text="Create Account  →")
        if ok:
            self.destroy()
            self._on_success()
        else:
            self._s_err.config(text=f"⚠  {msg}")

    # ── Forgot Password ───────────────────────────────────────────────────────
    def _forgot_pw(self):
        import random
        _otp = [None]

        win = tk.Toplevel(self)
        win.title("Reset Password")
        win.resizable(False, False)
        win.configure(bg=BG)
        win.grab_set()

        def _center_win(w, h):
            win.update_idletasks()
            sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
            win.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

        # ── STEP 1: Email input ───────────────────────────────────────────────
        def _step1():
            for c in win.winfo_children(): c.destroy()
            _center_win(400, 300)

            tk.Label(win, text="🔑  Forgot Password",
                     font=("Helvetica", 14, "bold"),
                     fg=WHITE, bg=BG).pack(pady=(24, 4))
            tk.Label(win,
                     text="Enter your registered email address.",
                     font=("Helvetica", 9), fg=MUTED, bg=BG).pack()

            frm = tk.Frame(win, bg=BG, padx=34)
            frm.pack(fill="x", pady=14)

            tk.Label(frm, text="Email Address",
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            em_v = tk.StringVar(value=self._l_email.get())
            tk.Entry(frm, textvariable=em_v,
                     font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE, relief="flat", bd=0
                     ).pack(fill="x", ipady=8, pady=(3, 0))

            st = tk.Label(frm, text="", font=("Helvetica", 8),
                           fg=ACCENT, bg=BG)
            st.pack(pady=(6, 0))

            def _send():
                email = em_v.get().strip().lower()
                if not email:
                    st.config(text="Email daalo.", fg=ACCENT); return
                conn = _auth_conn()
                row  = conn.execute(
                    "SELECT id FROM users WHERE email=?",
                    (email,)).fetchone()
                conn.close()
                if not row:
                    st.config(text="Yeh email registered nahi hai.", fg=ACCENT)
                    return
                otp = str(random.randint(100000, 999999))
                _otp[0] = otp
                body = (
                    f"Your FastTube Pro password reset code:\n\n"
                    f"    OTP: {otp}\n\n"
                    f"This code expires in 10 minutes.\n"
                    f"If you did not request this, ignore this email.\n\n"
                    f"— FastTube Pro Team"
                )
                threading.Thread(
                    target=_send_email,
                    args=(email, "FastTube Pro — Password Reset OTP", body),
                    daemon=True).start()
                st.config(text=f"OTP sent to {email}", fg=GREEN)
                win.after(1000, lambda: _step2(email))

            tk.Button(frm, text="Send Reset Code",
                      font=("Helvetica", 10, "bold"), bg=ACCENT, fg=WHITE,
                      relief="flat", bd=0, pady=10, cursor="hand2",
                      command=_send).pack(fill="x", pady=(10, 0))

        # ── STEP 2: OTP + new password ────────────────────────────────────────
        def _step2(email):
            for c in win.winfo_children(): c.destroy()
            _center_win(400, 380)

            tk.Label(win, text="🔑  Enter Reset Code",
                     font=("Helvetica", 14, "bold"),
                     fg=WHITE, bg=BG).pack(pady=(24, 4))
            tk.Label(win, text=f"Code sent to: {email}",
                     font=("Helvetica", 8), fg=MUTED, bg=BG).pack()

            frm = tk.Frame(win, bg=BG, padx=34)
            frm.pack(fill="x", pady=14)

            tk.Label(frm, text="6-Digit OTP",
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            otp_v = tk.StringVar()
            tk.Entry(frm, textvariable=otp_v,
                     font=("Helvetica", 14, "bold"), bg=BG_INPUT,
                     fg=YELLOW, insertbackground=WHITE,
                     relief="flat", bd=0, justify="center"
                     ).pack(fill="x", ipady=10, pady=(3, 10))

            tk.Label(frm, text="New Password  (min 6 chars)",
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            npw_v = tk.StringVar()
            tk.Entry(frm, textvariable=npw_v, show="●",
                     font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE, relief="flat", bd=0
                     ).pack(fill="x", ipady=8, pady=(3, 10))

            tk.Label(frm, text="Confirm New Password",
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            cpw_v = tk.StringVar()
            tk.Entry(frm, textvariable=cpw_v, show="●",
                     font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE, relief="flat", bd=0
                     ).pack(fill="x", ipady=8, pady=(3, 0))

            st = tk.Label(frm, text="", font=("Helvetica", 8),
                           fg=ACCENT, bg=BG, wraplength=320)
            st.pack(pady=(6, 0))

            def _reset():
                if otp_v.get().strip() != _otp[0]:
                    st.config(text="OTP galat hai. Check karo.", fg=ACCENT)
                    return
                npw = npw_v.get()
                cpw = cpw_v.get()
                if len(npw) < 6:
                    st.config(text="Password minimum 6 characters.", fg=ACCENT)
                    return
                if npw != cpw:
                    st.config(text="Passwords match nahi kar rahe.", fg=ACCENT)
                    return
                conn = _auth_conn()
                conn.execute(
                    "UPDATE users SET password_hash=? WHERE email=?",
                    (_hash_pw(npw), email))
                conn.commit(); conn.close()
                st.config(text="Password reset ho gaya! Login karo.", fg=GREEN)
                win.after(1800, win.destroy)

            tk.Button(frm, text="Reset Password  ✅",
                      font=("Helvetica", 10, "bold"), bg=GREEN, fg=BG,
                      relief="flat", bd=0, pady=10, cursor="hand2",
                      command=_reset).pack(fill="x", pady=(10, 0))

            wrong_lbl = tk.Label(frm, text="Wrong email?",
                     font=("Helvetica", 8), fg=MUTED,
                     bg=BG, cursor="hand2")
            wrong_lbl.pack(pady=(8, 0))
            wrong_lbl.bind("<Button-1>", lambda e: _step1())

        _step1()

class UpgradeDialog(tk.Toplevel):
    def __init__(self, master, on_done=None):
        super().__init__(master)
        self._on_done = on_done
        self.title("Plans & Billing — FastTube Pro")
        self.geometry("900x580")
        self.resizable(False,False)
        self.configure(bg=BG)
        self.grab_set()
        self._build()
        self._center()

    def _center(self):
        self.update_idletasks()
        w,h = self.winfo_width(), self.winfo_height()
        sw,sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _build(self):
        hdr = tk.Frame(self, bg=BG, pady=16)
        hdr.pack(fill="x")
        tk.Label(hdr, text="⚡ Plans & Billing",
                 font=("Helvetica",16,"bold"), fg=WHITE, bg=BG).pack()
        tk.Label(hdr, text="Choose your plan · No hidden charges",
                 font=("Helvetica",9), fg=MUTED, bg=BG).pack(pady=(2,0))

        cards = tk.Frame(self, bg=BG)
        cards.pack(fill="both", expand=True, padx=16, pady=(0,14))

        # Load plans from DB (admin-editable)
        plan_data = get_all_plan_settings()
        plan_order = ["FREE", "DAILY", "MONTHLY", "YEARLY"]
        cur = _S.get("plan", "FREE")

        col_map = {"FREE": MUTED, "DAILY": BLUE, "MONTHLY": GREEN, "YEARLY": YELLOW}

        for pk in plan_order:
            d = plan_data.get(pk)
            if not d:
                continue
            d["col"] = col_map.get(pk, MUTED)
            self._card(cards, d, pk == cur)

        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")
        ft = tk.Frame(self, bg=BG, pady=8)
        ft.pack(fill="x")
        exp = _S.get("expiry","")
        exp_txt = f"  Expires: {exp[:10]}" if exp else ""
        tk.Label(ft,
            text=f"Logged in: {_S.get('name','')}  ({_S.get('email','')})"
                 f"   |   Current: {cur}{exp_txt}",
            font=("Helvetica",8), fg=MUTED, bg=BG).pack(side="left",padx=14)
        tk.Button(ft, text="✖ Close", font=("Helvetica",8,"bold"),
            bg=DIM, fg=MUTED, relief="flat", bd=0, padx=10, pady=4,
            cursor="hand2", command=self.destroy).pack(side="right",padx=14)

    def _card(self, parent, d, is_cur):
        col   = d.get("col", MUTED)
        outer = tk.Frame(parent,
            bg=BG_CARD,
            highlightbackground=col if is_cur else BORDER,
            highlightthickness=2 if is_cur else 1)
        outer.pack(side="left", fill="both", expand=True, padx=5, pady=4)
        inn = tk.Frame(outer, bg=BG_CARD, padx=12, pady=10)
        inn.pack(fill="both", expand=True)

        if d.get("badge"):
            tk.Label(inn, text=f" {d['badge']} ",
                font=("Helvetica",7,"bold"),
                bg=col, fg=BG if col in (YELLOW,GREEN) else WHITE,
                padx=4, pady=2).pack(anchor="w")
        else:
            tk.Label(inn, text="", bg=BG_CARD).pack()

        tk.Label(inn, text=d["name"],
            font=("Helvetica",14,"bold"), fg=col, bg=BG_CARD).pack(anchor="w", pady=(4,0))

        if d.get("orig"):
            pr = tk.Frame(inn, bg=BG_CARD); pr.pack(anchor="w")
            tk.Label(pr, text=d["orig"],
                font=("Helvetica",8), fg=MUTED, bg=BG_CARD).pack(side="left")
            tk.Label(pr, text=" ✕",
                font=("Helvetica",8), fg=ACCENT, bg=BG_CARD).pack(side="left")

        tk.Label(inn, text=d["price"],
            font=("Helvetica",20,"bold"), fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(inn, text=d["sub"],
            font=("Helvetica",7), fg=col, bg=BG_CARD).pack(anchor="w", pady=(0,6))
        tk.Frame(inn, bg=BORDER, height=1).pack(fill="x", pady=(0,6))

        # Feature list — DB driven, ✅ = enabled, ❌ = disabled
        for f in d.get("feats", []):
            if not f:
                continue
            enabled_feat = f.startswith("✅")
            disabled_feat = f.startswith("❌")
            fg_col = WHITE if enabled_feat else (ACCENT if disabled_feat else MUTED)
            tk.Label(inn, text=f"  {f}",
                font=("Helvetica",8),
                fg=fg_col,
                bg=BG_CARD, anchor="w").pack(fill="x", pady=1)

        if is_cur:
            tk.Label(inn, text="✅ Current Plan",
                font=("Helvetica",9,"bold"), fg=col, bg=BG_CARD
                ).pack(fill="x", pady=(8,0))
        else:
            tk.Button(inn, text="Activate",
                font=("Helvetica",10,"bold"),
                bg=col if col != MUTED else DIM,
                fg=BG if col in (YELLOW,GREEN) else WHITE,
                relief="flat", bd=0, pady=7, cursor="hand2",
                command=lambda k=d["key"]: self._activate(k)
                ).pack(fill="x", pady=(8,0))

    def _activate(self, key):
        price = PLANS[key]["price"]
        name  = PLANS[key]["name"]
        if price > 0:
            UPIPaymentDialog(self, key, name, price,
                             on_paid=self._on_paid)
        else:
            # Free plan — activate directly
            sub_activate(key)
            messagebox.showinfo("✅ Activated",
                "Free plan activated!", parent=self)
            self.destroy()
            if self._on_done: self._on_done()

    def _on_paid(self, key):
        """Called after UPI UTR submitted — do NOT activate, wait for admin."""
        # ❌ sub_activate(key)  — removed: plan activates only on admin approval
        self.destroy()
        # Refresh UI so badge still shows current (unchanged) plan
        if self._on_done: self._on_done()



# ══════════════════════════════════════════════════════════════════════════════
#  UPI PAYMENT DIALOG
# ══════════════════════════════════════════════════════════════════════════════
# Your UPI ID — change karo agar alag hai
ADMIN_UPI_NAME = "FastTube Pro"

# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN PANEL  (password-protected — for app owner only)
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ══════════════════════════════════════════════════════════════════════════════
class AdminPanel(tk.Toplevel):
    """Admin Panel: Signup (first run) -> Login -> Dashboard -> Payments"""

    def __init__(self, master):
        super().__init__(master)
        self.title("FastTube Pro - Admin")
        self.configure(bg=BG)
        self.resizable(True, True)
        self.grab_set()
        self._admin_id   = None
        self._admin_name = ""
        self._ensure_tables()
        if self._admin_count() == 0:
            self._show_signup()
        else:
            self._show_login()
        self._center()

    def _center(self):
        self.update_idletasks()
        w,h = self.winfo_width(), self.winfo_height()
        sw,sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(str(w)+"x"+str(h)+"+"+str((sw-w)//2)+"+"+str((sh-h)//2))

    def _clear(self):
        for w in self.winfo_children(): w.destroy()

    def _resize(self, geo):
        self.geometry(geo)
        self._center()

    def _ensure_tables(self):
        conn = _auth_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS upi_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT, user_name TEXT,
            plan TEXT, price INTEGER, utr TEXT,
            status TEXT DEFAULT 'pending',
            submitted_at TEXT DEFAULT (datetime('now'))
        )""")
        conn.commit(); conn.close()

    def _admin_count(self):
        conn = _auth_conn()
        n = conn.execute("SELECT COUNT(*) FROM admins").fetchone()[0]
        conn.close()
        return n

    def _verify_admin(self, email, pw):
        conn = _auth_conn()
        row  = conn.execute(
            "SELECT id,name FROM admins WHERE email=? AND password_hash=?",
            (email.strip().lower(), _hash_pw(pw))).fetchone()
        conn.close()
        return row

    def _hdr(self, title, sub=""):
        tk.Label(self, text=title,
                 font=("Helvetica",15,"bold"),
                 fg=ACCENT, bg=BG).pack(pady=(22,2))
        if sub:
            tk.Label(self, text=sub,
                     font=("Helvetica",8), fg=MUTED, bg=BG).pack()
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x", pady=(10,0))

    def _field(self, parent, label, show=""):
        tk.Label(parent, text=label,
                 font=("Helvetica",8,"bold"),
                 fg=MUTED, bg=BG).pack(anchor="w")
        row = tk.Frame(parent, bg=BG_INPUT)
        row.pack(fill="x", pady=(3,10))
        v = tk.StringVar()
        e = tk.Entry(row, textvariable=v, show=show,
                     font=("Helvetica",10),
                     bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE,
                     relief="flat", bd=0)
        e.pack(side="left", fill="x", expand=True, ipady=8, padx=(8,0))
        if show:
            def _tog(en=e):
                en.config(show="" if en.cget("show") else "bullet")
            tk.Button(row, text="show", font=("Helvetica",9),
                      bg=BG_INPUT, fg=MUTED, relief="flat", bd=0,
                      cursor="hand2", command=_tog).pack(side="right", padx=4)
        return v

    def _stlbl(self, parent):
        lbl = tk.Label(parent, text="",
                       font=("Helvetica",8), fg=ACCENT, bg=BG,
                       wraplength=340)
        lbl.pack(pady=(4,0))
        return lbl

    def _show_signup(self):
        self._clear(); self._resize("420x460")
        self._hdr("Admin Setup", "Pehli baar - admin account banao")
        frm = tk.Frame(self, bg=BG, padx=36)
        frm.pack(fill="x", pady=12)
        n_v  = self._field(frm, "Full Name")
        em_v = self._field(frm, "Email Address")
        pw_v = self._field(frm, "Password (min 6 chars)", show="*")
        cp_v = self._field(frm, "Confirm Password", show="*")
        st   = self._stlbl(frm)

        def _do():
            n  = n_v.get().strip()
            em = em_v.get().strip().lower()
            pw = pw_v.get()
            cp = cp_v.get()
            if not n or not em:
                st.config(text="Name aur email zaroori."); return
            if "@" not in em:
                st.config(text="Valid email daalo."); return
            if len(pw) < 6:
                st.config(text="Password min 6 chars."); return
            if pw != cp:
                st.config(text="Passwords match nahi."); return
            try:
                conn = _auth_conn()
                conn.execute(
                    "INSERT INTO admins(name,email,password_hash) VALUES(?,?,?)",
                    (n, em, _hash_pw(pw)))
                conn.commit(); conn.close()
                st.config(text="Admin account bana! Login karo.", fg=GREEN)
                self.after(1200, self._show_login)
            except Exception as ex:
                st.config(text="Error: "+str(ex))

        tk.Button(frm, text="Create Admin Account",
                  font=("Helvetica",10,"bold"),
                  bg=ACCENT, fg=WHITE, relief="flat", bd=0,
                  pady=10, cursor="hand2",
                  command=_do).pack(fill="x", pady=(6,0))
        self.bind("<Return>", lambda e: _do())

    def _show_login(self):
        self._clear(); self._resize("420x340")
        self._hdr("Admin Login", "FastTube Pro - Sirf Owner ke liye")
        frm = tk.Frame(self, bg=BG, padx=36)
        frm.pack(fill="x", pady=14)
        em_v = self._field(frm, "Admin Email")
        pw_v = self._field(frm, "Password", show="*")
        st   = self._stlbl(frm)

        def _do():
            row = self._verify_admin(em_v.get(), pw_v.get())
            if row:
                self._admin_id, self._admin_name = row
                self._show_dashboard()
            else:
                st.config(text="Email ya password galat.")
                pw_v.set("")

        tk.Button(frm, text="Login",
                  font=("Helvetica",11,"bold"),
                  bg=ACCENT, fg=WHITE, relief="flat", bd=0,
                  pady=10, cursor="hand2",
                  command=_do).pack(fill="x", pady=(8,0))
        self.bind("<Return>", lambda e: _do())

    def _show_dashboard(self, filter_plan="ALL"):
        self._clear(); self._resize("980x700")

        # ── Fetch counts ──────────────────────────────────────────────────────
        try:
            conn = _auth_conn()
            total     = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            n_free    = conn.execute("SELECT COUNT(*) FROM user_subscription WHERE plan_type='FREE'    AND status='ACTIVE'").fetchone()[0]
            n_daily   = conn.execute("SELECT COUNT(*) FROM user_subscription WHERE plan_type='DAILY'   AND status='ACTIVE'").fetchone()[0]
            n_monthly = conn.execute("SELECT COUNT(*) FROM user_subscription WHERE plan_type='MONTHLY' AND status='ACTIVE'").fetchone()[0]
            n_yearly  = conn.execute("SELECT COUNT(*) FROM user_subscription WHERE plan_type='YEARLY'  AND status='ACTIVE'").fetchone()[0]
            pend      = conn.execute("SELECT COUNT(*) FROM upi_payments WHERE status='pending'").fetchone()[0]
            conn.close()
        except Exception:
            total = n_free = n_daily = n_monthly = n_yearly = pend = 0

        count_map   = {"ALL": total, "FREE": n_free, "DAILY": n_daily,
                       "MONTHLY": n_monthly, "YEARLY": n_yearly}
        _opt_labels = {
            "ALL":     "ALL  ({})".format(total),
            "FREE":    "FREE  ({})".format(n_free),
            "DAILY":   "DAILY  ({})".format(n_daily),
            "MONTHLY": "MONTHLY  ({})".format(n_monthly),
            "YEARLY":  "YEARLY  ({})".format(n_yearly),
        }
        _label_to_key  = {v: k for k, v in _opt_labels.items()}
        _combo_values  = [_opt_labels[k] for k in ["ALL","FREE","DAILY","MONTHLY","YEARLY"]]
        _filt_colors   = {"ALL": WHITE, "FREE": MUTED, "DAILY": BLUE,
                          "MONTHLY": GREEN, "YEARLY": YELLOW}

        # ════ TOP BAR ════════════════════════════════════════════════════════
        bar = tk.Frame(self, bg=BG, pady=8)
        bar.pack(fill="x")

        # ── LEFT: Title ───────────────────────────────────────────────────────
        tk.Label(bar, text="🔐  Admin Dashboard — FastTube Pro",
                 font=("Helvetica", 12, "bold"),
                 fg=ACCENT, bg=BG).pack(side="left", padx=(14, 8))

        # ── RIGHT: Filter + action buttons (all in one row) ───────────────────
        right_bar = tk.Frame(bar, bg=BG)
        right_bar.pack(side="right", padx=10)

        # Filter: label + combobox
        tk.Label(right_bar, text="Filter:",
                 font=("Helvetica", 8, "bold"),
                 fg=MUTED, bg=BG).pack(side="left", padx=(0, 4))

        # Style the combobox to match dark theme
        _s = ttk.Style()
        _s.configure("Admin.TCombobox",
                     fieldbackground=BG_INPUT, background=BG_INPUT,
                     foreground=WHITE, bordercolor=BORDER,
                     arrowcolor=WHITE,
                     selectbackground=ACCENT, selectforeground=WHITE)

        filter_disp = tk.StringVar(value=_opt_labels.get(filter_plan, _opt_labels["ALL"]))
        filter_combo = ttk.Combobox(right_bar,
                                    textvariable=filter_disp,
                                    values=_combo_values,
                                    width=17, state="readonly",
                                    style="Admin.TCombobox",
                                    font=("Helvetica", 9, "bold"))
        filter_combo.pack(side="left", padx=(0, 8))

        def _on_filter(event=None):
            key = _label_to_key.get(filter_disp.get(), "ALL")
            self._show_dashboard(key)
        filter_combo.bind("<<ComboboxSelected>>", _on_filter)

        # Thin divider between filter and buttons
        tk.Frame(right_bar, bg=BORDER, width=1).pack(
            side="left", fill="y", pady=3, padx=(0, 8))

        # Action buttons
        for rtxt, rcmd, rbg, rfg in [
            ("🔄 Refresh",      lambda fp=filter_plan: self._show_dashboard(fp), BG_CARD,   MUTED),
            ("💳 Payments",     self._show_payments,                              BLUE,       WHITE),
            ("⚙ Plan Limits",   self._show_plan_limits,                           "#1a3a1a",  GREEN),
            ("🔑 Change PW",    self._change_pw,                                  BG_CARD,   MUTED),
            ("↩ Logout",        self._show_login,                                 "#2a0a0a", ACCENT),
        ]:
            tk.Button(right_bar, text=rtxt,
                      font=("Helvetica", 8), bg=rbg, fg=rfg,
                      relief="flat", bd=0,
                      padx=10, pady=5, cursor="hand2",
                      command=rcmd).pack(side="left", padx=2)

        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

        # ════ STATS BAR ══════════════════════════════════════════════════════
        sbar = tk.Frame(self, bg=BG_CARD, pady=8)
        sbar.pack(fill="x", padx=14, pady=(10,0))
        for slbl, sval, scol in [
            ("Total Users", total,     WHITE),
            ("Free",        n_free,    MUTED),
            ("Daily",       n_daily,   BLUE),
            ("Monthly",     n_monthly, GREEN),
            ("Yearly",      n_yearly,  YELLOW),
            ("Pending",     pend,      ACCENT if pend > 0 else MUTED),
        ]:
            sf = tk.Frame(sbar, bg=BG_CARD)
            sf.pack(side="left", expand=True)
            tk.Label(sf, text=str(sval),
                     font=("Helvetica",18,"bold"),
                     fg=scol, bg=BG_CARD).pack()
            tk.Label(sf, text=slbl,
                     font=("Helvetica",7), fg=MUTED, bg=BG_CARD).pack()

        # ════ EXPIRY WARNING ═════════════════════════════════════════════════
        try:
            ce = _auth_conn()
            exp_soon = ce.execute(
                "SELECT u.name, u.email, s.plan_type, s.expiry_date "
                "FROM users u "
                "JOIN (SELECT user_id, plan_type, expiry_date, status "
                "      FROM user_subscription "
                "      WHERE id IN (SELECT MAX(id) FROM user_subscription GROUP BY user_id)) s "
                "ON s.user_id = u.id "
                "WHERE s.expiry_date IS NOT NULL AND s.expiry_date != '' "
                "AND s.status='ACTIVE' "
                "AND date(s.expiry_date) BETWEEN date('now') AND date('now','+5 days') "
                "ORDER BY s.expiry_date ASC LIMIT 6"
            ).fetchall()
            ce.close()
        except Exception:
            exp_soon = []

        if exp_soon:
            wf = tk.Frame(self, bg="#2a1500", padx=14, pady=4)
            wf.pack(fill="x", padx=14, pady=(6,0))
            tk.Label(wf, text="Expiring soon:",
                     font=("Helvetica",8,"bold"),
                     fg=ORANGE, bg="#2a1500").pack(side="left")
            for nm, em, pl, ex in exp_soon:
                tk.Label(wf,
                         text="  - {} [{}] -> {}".format(nm, pl, ex[:10] if ex else "?"),
                         font=("Helvetica",8), fg=YELLOW, bg="#2a1500"
                         ).pack(side="left", padx=4)

        # Active filter label
        sec_f = tk.Frame(self, bg=BG, padx=14)
        sec_f.pack(fill="x", pady=(6, 2))
        sec_col = _filt_colors.get(filter_plan, WHITE)
        tk.Label(sec_f,
                 text="  Showing: {}  -  {} record(s)".format(
                     filter_plan, count_map.get(filter_plan, total)),
                 font=("Helvetica",9,"bold"),
                 fg=sec_col, bg=BG_INPUT, padx=10, pady=3, anchor="w"
                 ).pack(fill="x")

        # ════ USERS TABLE ════════════════════════════════════════════════════
        # KEY FIX: Subquery picks ONLY latest subscription per user
        # This prevents duplicate rows AND ensures name/email always show
        tv_style()
        tf = tk.Frame(self, bg=BG)
        tf.pack(fill="both", expand=True, padx=14, pady=(4,0))

        tcols   = ("#", "Name", "Email", "Plan", "Expiry", "Downloads", "Joined")
        twidths = [35,  135,    195,     80,     120,       65,          95]
        tree = ttk.Treeview(tf, columns=tcols, show="headings", height=13)
        for c, w in zip(tcols, twidths):
            tree.heading(c, text=c)
            tree.column(c, width=w,
                        anchor="center" if c == "#" else "w",
                        minwidth=30)

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side="right",  fill="y")
        hsb.pack(side="bottom", fill="x")
        tree.pack(fill="both",  expand=True)

        # Build safe WHERE addition
        _safe_plans = {"FREE", "DAILY", "MONTHLY", "YEARLY"}
        if filter_plan in _safe_plans:
            extra_w = "AND COALESCE(s.plan_type,'FREE') = '{}' ".format(filter_plan)
        else:
            extra_w = ""

        sql = (
            "SELECT u.id, u.name, u.email, "
            "COALESCE(s.plan_type,'FREE') AS plan, "
            "COALESCE(s.expiry_date,'') AS expiry, "
            "COALESCE(s.daily_count,0)+COALESCE(s.weekly_count,0) AS dls, "
            "u.created_at "
            "FROM users u "
            "LEFT JOIN ("
            "  SELECT user_id, plan_type, expiry_date, daily_count, weekly_count "
            "  FROM user_subscription "
            "  WHERE id IN (SELECT MAX(id) FROM user_subscription GROUP BY user_id)"
            ") s ON s.user_id = u.id "
            "WHERE 1=1 " + extra_w +
            "ORDER BY u.id DESC"
        )

        try:
            cd = _auth_conn()
            rows = cd.execute(sql).fetchall()
            cd.close()
        except Exception as dbe:
            rows = []
            tree.insert("", "end",
                values=("", "DB Error: " + str(dbe)[:70],
                        "", "", "", "", ""))

        today = datetime.now().date()
        for rn, row in enumerate(rows, 1):
            uid, uname, uemail, uplan, uexp, udls, ujoined = row

            # Expiry display logic
            etag = (uplan or "free").lower()
            if uexp:
                try:
                    exp_date = datetime.fromisoformat(uexp[:10]).date()
                    diff = (exp_date - today).days
                    if diff < 0:
                        exp_disp = "EXPIRED ({})".format(uexp[:10])
                        etag = "expired"
                    elif diff == 0:
                        exp_disp = "Today! ({})".format(uexp[:10])
                        etag = "expiring"
                    elif diff <= 3:
                        exp_disp = "{} ({}d left)".format(uexp[:10], diff)
                        etag = "expiring"
                    else:
                        exp_disp = uexp[:10]
                except Exception:
                    exp_disp = uexp[:10] if uexp else "—"
            else:
                exp_disp = "Forever"

            disp_name  = str(uname).strip()  if uname  else "(no name)"
            disp_email = str(uemail).strip() if uemail else "(no email)"

            tree.insert("", "end",
                values=(rn, disp_name, disp_email,
                        uplan or "FREE", exp_disp,
                        udls if udls else 0,
                        ujoined[:10] if ujoined else ""),
                tags=(etag,))

        tree.tag_configure("free",     foreground=MUTED)
        tree.tag_configure("daily",    foreground=BLUE)
        tree.tag_configure("monthly",  foreground=GREEN)
        tree.tag_configure("yearly",   foreground=YELLOW)
        tree.tag_configure("expired",  foreground=ACCENT,  background="#2a0a0a")
        tree.tag_configure("expiring", foreground=ORANGE,  background="#2a1500")

        # ════ ACTION BAR ═════════════════════════════════════════════════════
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x", pady=(4,0))
        act = tk.Frame(self, bg=BG_CARD, pady=10, padx=14)
        act.pack(fill="x")

        tk.Label(act,
                 text="Table mein user select karo  ->  Plan chuno  ->  Action karo",
                 font=("Helvetica",8), fg=MUTED, bg=BG_CARD
                 ).pack(anchor="w", pady=(0,5))

        btn_row = tk.Frame(act, bg=BG_CARD)
        btn_row.pack(anchor="w")

        tk.Label(btn_row, text="Plan:",
                 font=("Helvetica",9,"bold"),
                 fg=WHITE, bg=BG_CARD).pack(side="left", padx=(0,6))

        plan_var = tk.StringVar(value="MONTHLY")
        ttk.Combobox(btn_row, textvariable=plan_var,
                     values=["FREE","DAILY","MONTHLY","YEARLY"],
                     width=12, state="readonly",
                     font=("Helvetica",9,"bold")
                     ).pack(side="left", padx=(0,12))

        def _do_renew():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("No Selection",
                    "Pehle table mein ek user pe click karo!", parent=self)
                return
            vals      = tree.item(sel[0])["values"]
            sel_name  = str(vals[1])
            sel_email = str(vals[2])
            new_plan  = plan_var.get()
            if not messagebox.askyesno("Confirm Renewal",
                "Plan activate karna chahte ho?\n\n"
                "  User  :  {}\n"
                "  Email :  {}\n"
                "  Plan  :  {}".format(sel_name, sel_email, new_plan),
                parent=self):
                return
            try:
                cr = _auth_conn()
                uid_row = cr.execute(
                    "SELECT id FROM users WHERE email=?", (sel_email,)).fetchone()
                cr.close()
                if not uid_row:
                    messagebox.showerror("Error",
                        "User not found in DB:\n" + sel_email, parent=self)
                    return
                orig_uid  = _S.get("uid")
                orig_plan = _S.get("plan")
                _S["uid"]  = uid_row[0]
                sub_activate(new_plan)
                _S["uid"]  = orig_uid
                _S["plan"] = orig_plan or "FREE"
                cr2 = _auth_conn()
                ap_n = cr2.execute(
                    "SELECT COUNT(*) FROM upi_payments WHERE user_email=? AND status='pending'",
                    (sel_email,)).fetchone()[0]
                cr2.execute(
                    "UPDATE upi_payments SET status='approved' WHERE user_email=? AND status='pending'",
                    (sel_email,))
                cr2.commit(); cr2.close()
                info = "{} activated for {}!".format(new_plan, sel_name)
                if ap_n:
                    info += "\n{} pending payment(s) auto-approved.".format(ap_n)
                messagebox.showinfo("Done", info, parent=self)
                self._show_dashboard(filter_plan)
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        def _do_delete():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("No Selection",
                    "Pehle table mein ek user pe click karo!", parent=self)
                return
            vals      = tree.item(sel[0])["values"]
            sel_name  = str(vals[1])
            sel_email = str(vals[2])
            if not messagebox.askyesno("Confirm Delete",
                "DELETE karna chahte ho?\n\n"
                "  Name  :  {}\n"
                "  Email :  {}\n\n"
                "Yeh UNDO nahi hoga!".format(sel_name, sel_email),
                parent=self):
                return
            try:
                cd2 = _auth_conn()
                uid_row = cd2.execute(
                    "SELECT id FROM users WHERE email=?", (sel_email,)).fetchone()
                if uid_row:
                    cd2.execute("DELETE FROM user_subscription WHERE user_id=?", (uid_row[0],))
                    cd2.execute("DELETE FROM users WHERE id=?",                  (uid_row[0],))
                    cd2.execute("DELETE FROM upi_payments WHERE user_email=?",   (sel_email,))
                cd2.commit(); cd2.close()
                messagebox.showinfo("Deleted",
                    "{} ({})\ndelete ho gaya.".format(sel_name, sel_email), parent=self)
                self._show_dashboard(filter_plan)
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        def _do_reset_usage():
            sel = tree.selection()
            if not sel:
                messagebox.showwarning("No Selection",
                    "Pehle table mein ek user pe click karo!", parent=self)
                return
            vals      = tree.item(sel[0])["values"]
            sel_name  = str(vals[1])
            sel_email = str(vals[2])
            if not messagebox.askyesno("Reset Feature Usage",
                "Iss user ke saare feature usage counters reset karein?\n\n"
                "  User  :  {}\n"
                "  Email :  {}\n\n"
                "User ab apne plan ki full limit se use kar sakta hai.".format(
                    sel_name, sel_email), parent=self):
                return
            try:
                cd3 = _auth_conn()
                uid_row = cd3.execute(
                    "SELECT id FROM users WHERE email=?", (sel_email,)).fetchone()
                cd3.close()
                if uid_row:
                    ok = sub_reset_feature_usage(uid=uid_row[0])
                    if ok:
                        messagebox.showinfo("Done",
                            "{}\nFeature usage reset ho gaya ✅\nAb wo apne plan ki full limit use kar sakta hai.".format(sel_name),
                            parent=self)
                    else:
                        messagebox.showerror("Error", "Reset failed.", parent=self)
                else:
                    messagebox.showerror("Not Found", "User not found.", parent=self)
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        tk.Button(btn_row, text="Renew Plan",
                  font=("Helvetica",10,"bold"),
                  bg=GREEN, fg=BG, relief="flat", bd=0,
                  padx=18, pady=7, cursor="hand2",
                  command=_do_renew).pack(side="left", padx=(0,8))

        tk.Button(btn_row, text="🔄 Reset Usage",
                  font=("Helvetica",10,"bold"),
                  bg=ORANGE, fg=WHITE, relief="flat", bd=0,
                  padx=16, pady=7, cursor="hand2",
                  command=_do_reset_usage).pack(side="left", padx=(0,8))

        tk.Button(btn_row, text="Delete User",
                  font=("Helvetica",10,"bold"),
                  bg="#2a0a0a", fg=ACCENT, relief="flat", bd=0,
                  padx=16, pady=7, cursor="hand2",
                  command=_do_delete).pack(side="left")

        leg = tk.Frame(act, bg=BG_CARD)
        leg.pack(side="right")
        for sym, lcol in [("● Expired", ACCENT), ("● Expiring soon", ORANGE)]:
            tk.Label(leg, text=sym, font=("Helvetica",7),
                     fg=lcol, bg=BG_CARD).pack(anchor="e")

    def _show_payments(self):
        self._clear(); self._resize("820x540")
        bar = tk.Frame(self, bg=BG, pady=8)
        bar.pack(fill="x")
        tk.Label(bar, text="UPI Payment Submissions",
                 font=("Helvetica",12,"bold"),
                 fg=WHITE, bg=BG).pack(side="left", padx=14)
        for txt, cmd in [("Refresh", self._show_payments),
                          ("Back", self._show_dashboard)]:
            tk.Button(bar, text=txt, font=("Helvetica",8),
                      bg=BG_CARD, fg=MUTED, relief="flat", bd=0,
                      padx=10, pady=5, cursor="hand2",
                      command=cmd).pack(side="right", padx=4)
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

        tf = tk.Frame(self, bg=BG)
        tf.pack(fill="both", expand=True, padx=14, pady=10)
        pcols  = ("ID","User","Email","Plan","Amount","UTR","Date","Status")
        pwidths= [40,  100,   160,    80,    65,      130,  110,   80]
        ptree  = ttk.Treeview(tf, columns=pcols, show="headings", height=16)
        for c, w in zip(pcols, pwidths):
            ptree.heading(c, text=c)
            ptree.column(c, width=w, anchor="center")
        pvsb = ttk.Scrollbar(tf, orient="vertical", command=ptree.yview)
        ptree.configure(yscrollcommand=pvsb.set)
        pvsb.pack(side="right", fill="y")
        ptree.pack(fill="both", expand=True)

        try:
            cp = _auth_conn()
            pays = cp.execute(
                "SELECT p.id, u.name, p.user_email, p.plan, "
                "p.price, p.utr, p.submitted_at, p.status "
                "FROM upi_payments p "
                "LEFT JOIN users u ON u.email = p.user_email "
                "ORDER BY p.id DESC"
            ).fetchall()
            cp.close()
        except Exception:
            pays = []

        for pay in pays:
            pid, uname, uemail, pplan, amt, utr_val, pdate, pstatus = pay
            disp_name = str(uname).strip() if uname else uemail
            ptree.insert("", "end",
                values=(pid, disp_name, uemail, pplan or "",
                        amt or "", utr_val or "", pdate[:16] if pdate else "", pstatus or ""),
                tags=(pstatus or "pending",))

        ptree.tag_configure("pending",  foreground=YELLOW)
        ptree.tag_configure("approved", foreground=GREEN)
        ptree.tag_configure("rejected", foreground=ACCENT)

        act = tk.Frame(self, bg=BG, pady=8, padx=14)
        act.pack(fill="x")

        def _approve():
            sel = ptree.selection()
            if not sel: return
            vals  = ptree.item(sel[0])["values"]
            pid   = vals[0]
            uname = str(vals[1])
            email = str(vals[2])
            pplan = str(vals[3])   # plan column
            utr_shown = str(vals[5])
            try:
                conn = _auth_conn()
                uid  = conn.execute(
                    "SELECT id FROM users WHERE email=?", (email,)).fetchone()
                conn.close()
                if uid:
                    orig_uid  = _S.get("uid")
                    orig_plan = _S.get("plan")
                    _S["uid"]  = uid[0]
                    sub_activate(pplan)
                    _S["uid"]  = orig_uid
                    _S["plan"] = orig_plan or "FREE"
                conn2 = _auth_conn()
                conn2.execute(
                    "UPDATE upi_payments SET status='approved' WHERE id=?", (pid,))
                conn2.commit(); conn2.close()
                messagebox.showinfo("✅ Approved",
                    f"{pplan} activated for {uname}\nUTR: {utr_shown}", parent=self)
                self._show_payments()
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        def _reject():
            sel = ptree.selection()
            if not sel: return
            pid = ptree.item(sel[0])["values"][0]
            try:
                conn = _auth_conn()
                conn.execute(
                    "UPDATE upi_payments SET status='rejected' WHERE id=?", (pid,))
                conn.commit(); conn.close()
                self._show_payments()
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        def _delete():
            sel = ptree.selection()
            if not sel: return
            vals  = ptree.item(sel[0])["values"]
            pid   = vals[0]
            uname = str(vals[1])
            confirm = messagebox.askyesno(
                "Delete Record",
                f"ID {pid} — {uname} ka payment record permanently delete karein?",
                parent=self)
            if not confirm: return
            try:
                conn = _auth_conn()
                conn.execute("DELETE FROM upi_payments WHERE id=?", (pid,))
                conn.commit(); conn.close()
                self._show_payments()
            except Exception as ex:
                messagebox.showerror("Error", str(ex), parent=self)

        for txt, cmd, bg, fg in [
            ("✅ Approve + Activate", _approve, GREEN,     BG),
            ("✖ Reject",              _reject,  "#2a0a0a", ACCENT),
            ("🗑 Delete Record",       _delete,  "#1a0000", "#ff4444"),
        ]:
            tk.Button(act, text=txt, font=("Helvetica",9,"bold"),
                      bg=bg, fg=fg, relief="flat", bd=0,
                      padx=14, pady=6, cursor="hand2",
                      command=cmd).pack(side="left", padx=4)

        tk.Label(act,
                 text="  Row select karein → action button dabayein",
                 font=("Helvetica",7), fg=MUTED, bg=BG).pack(side="left")


    def _show_plan_limits(self):
        """Admin popup to edit plan settings + per-feature usage limits."""
        win = tk.Toplevel(self)
        win.title("⚙ Plan Settings & Feature Limits")
        win.geometry("920x640")
        win.resizable(True, True)
        win.configure(bg=BG)
        win.grab_set()

        # ── Header ────────────────────────────────────────────────────────
        hdr = tk.Frame(win, bg=BG, pady=10)
        hdr.pack(fill="x", padx=16)
        tk.Label(hdr, text="⚙ Plan Settings & Feature Limits",
                 font=("Helvetica",13,"bold"), fg=WHITE, bg=BG).pack(side="left")
        tk.Frame(win, bg=BORDER, height=1).pack(fill="x")

        # ── Tab selector ──────────────────────────────────────────────────
        tab_bar = tk.Frame(win, bg=BG_CARD, pady=0)
        tab_bar.pack(fill="x")
        tab_var = tk.StringVar(value="settings")
        content_frame = tk.Frame(win, bg=BG)
        content_frame.pack(fill="both", expand=True)

        plans      = ["FREE", "DAILY", "MONTHLY", "YEARLY"]
        plan_cols  = {"FREE": MUTED, "DAILY": BLUE, "MONTHLY": GREEN, "YEARLY": YELLOW}
        FEAT_LABELS = {
            "batch":"📦 Batch", "single":"⬇ Single", "audio":"🎵 Audio",
            "convert":"🔄 Convert", "subtitles":"📄 Subtitles",
            "metadata":"ℹ Metadata", "thumbnail":"🖼 Thumbnail",
            "4k":"🎬 4K", "8k":"🎬 8K",
        }
        FEAT_COLS  = ["feat_batch","feat_single","feat_audio","feat_convert",
                      "feat_subtitles","feat_metadata","feat_thumbnail","feat_4k","feat_8k"]
        FEAT_KEYS  = ["batch","single","audio","convert","subtitles","metadata","thumbnail","4k","8k"]

        # Load current settings
        conn = _auth_conn()
        _ensure_limits_table(conn)
        ps_rows = conn.execute("""
            SELECT plan_type,display_name,price,orig_price,validity_days,badge,enabled,
                   feat_batch,feat_single,feat_audio,feat_convert,feat_subtitles,
                   feat_metadata,feat_thumbnail,feat_4k,feat_8k,max_res,extra_notes
            FROM plan_settings ORDER BY price ASC""").fetchall()
        lim_rows = conn.execute(
            "SELECT plan_type,feature,max_uses,reset_days FROM plan_feature_limits"
        ).fetchall()
        conn.close()

        # Build dicts
        ps = {}
        for row in ps_rows:
            pt = row[0]
            ps[pt] = {
                "display_name": row[1], "price": row[2], "orig_price": row[3],
                "validity_days": row[4], "badge": row[5], "enabled": row[6],
                "feats": dict(zip(FEAT_KEYS, row[7:16])),
                "max_res": row[16], "extra_notes": row[17]
            }
        lims = {}
        for plan in plans:
            lims[plan] = {}
        for plan, feat, mx, rd in lim_rows:
            if plan in lims:
                lims[plan][feat] = (mx, rd)

        # Store all vars
        sv = {}  # sv[plan] = {field: StringVar/BooleanVar}
        lv = {}  # lv[plan][feat] = (mx_var, days_var)
        for plan in plans:
            sv[plan] = {}
            pd = ps.get(plan, {})
            sv[plan]["display_name"]  = tk.StringVar(value=str(pd.get("display_name", plan)))
            sv[plan]["price"]         = tk.StringVar(value=str(pd.get("price", 0)))
            sv[plan]["orig_price"]    = tk.StringVar(value=str(pd.get("orig_price", 0)))
            sv[plan]["validity_days"] = tk.StringVar(value=str(pd.get("validity_days", 0)))
            sv[plan]["badge"]         = tk.StringVar(value=str(pd.get("badge", "")))
            sv[plan]["enabled"]       = tk.BooleanVar(value=bool(pd.get("enabled", 1)))
            sv[plan]["max_res"]       = tk.StringVar(value=str(pd.get("max_res", 720)))
            sv[plan]["extra_notes"]   = tk.StringVar(value=str(pd.get("extra_notes", "")))
            feats_d = pd.get("feats", {})
            for fk in FEAT_KEYS:
                sv[plan][f"feat_{fk}"] = tk.BooleanVar(value=bool(feats_d.get(fk, 0)))
            lv[plan] = {}
            for feat in FEATURES:
                mx, rd = lims.get(plan, {}).get(feat, (1, 7))
                lv[plan][feat] = (tk.StringVar(value=str(mx)), tk.StringVar(value=str(rd)))

        # ── Page renderer ──────────────────────────────────────────────────
        def show_tab(tab):
            for w in content_frame.winfo_children():
                w.destroy()

            if tab == "settings":
                _build_settings_tab()
            else:
                _build_limits_tab()

        def _build_settings_tab():
            """Plan price, features enable/disable."""
            canvas = tk.Canvas(content_frame, bg=BG, highlightthickness=0)
            vsb    = ttk.Scrollbar(content_frame, orient="vertical", command=canvas.yview)
            canvas.configure(yscrollcommand=vsb.set)
            vsb.pack(side="right", fill="y")
            canvas.pack(side="left", fill="both", expand=True)
            inner  = tk.Frame(canvas, bg=BG)
            cw     = canvas.create_window((0,0), window=inner, anchor="nw")
            inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.bind("<Configure>", lambda e: canvas.itemconfig(cw, width=e.width))
            inner.bind("<MouseWheel>", lambda e: canvas.yview_scroll(-1*(e.delta//120),"units"))

            # Column headers
            tk.Label(inner, text="Setting", font=("Helvetica",9,"bold"),
                     fg=WHITE, bg=BG, width=16, anchor="w").grid(row=0, column=0, padx=8, pady=6, sticky="w")
            for ci, plan in enumerate(plans):
                tk.Label(inner, text=plan, font=("Helvetica",9,"bold"),
                         fg=plan_cols[plan], bg=BG_CARD, padx=12, pady=4).grid(
                         row=0, column=ci+1, padx=6, pady=6, sticky="ew")

            rows_cfg = [
                ("display_name",  "Plan Name",       "entry"),
                ("price",         "Price (₹)",       "entry"),
                ("orig_price",    "Original Price ₹","entry"),
                ("validity_days", "Validity (days)",  "entry"),
                ("badge",         "Badge Text",       "entry"),
                ("max_res",       "Max Resolution",   "entry"),
                ("extra_notes",   "Subtitle Text",    "entry"),
                ("enabled",       "Plan Enabled",     "check"),
            ]

            for ri, (field, label, wtype) in enumerate(rows_cfg):
                tk.Frame(inner, bg=BORDER, height=1).grid(
                    row=ri*2+1, column=0, columnspan=5, sticky="ew", padx=4)
                tk.Label(inner, text=label, font=("Helvetica",8,"bold"),
                         fg=MUTED, bg=BG, width=16, anchor="w").grid(
                         row=ri*2+2, column=0, padx=8, pady=6, sticky="w")
                for ci, plan in enumerate(plans):
                    if wtype == "entry":
                        e = tk.Entry(inner, textvariable=sv[plan][field],
                                     width=14, bg=BG_INPUT, fg=WHITE,
                                     font=("Helvetica",9), relief="flat",
                                     insertbackground=WHITE)
                        e.grid(row=ri*2+2, column=ci+1, padx=6, pady=4)
                    else:
                        tk.Checkbutton(inner, variable=sv[plan][field],
                                       bg=BG, fg=WHITE, selectcolor=BG_INPUT,
                                       activebackground=BG,
                                       text="Enabled").grid(
                                       row=ri*2+2, column=ci+1, padx=6, pady=4)

            # Feature toggles section
            sep_row = len(rows_cfg)*2 + 2
            tk.Frame(inner, bg=BORDER, height=2).grid(
                row=sep_row, column=0, columnspan=5, sticky="ew", padx=4, pady=(10,4))
            tk.Label(inner, text="── Feature Access ──", font=("Helvetica",9,"bold"),
                     fg=WHITE, bg=BG).grid(row=sep_row+1, column=0, columnspan=5, pady=4)

            for fi, (fk, fl) in enumerate(FEAT_LABELS.items()):
                row_i = sep_row + 2 + fi*2
                tk.Frame(inner, bg=BORDER, height=1).grid(
                    row=row_i, column=0, columnspan=5, sticky="ew", padx=4)
                tk.Label(inner, text=fl, font=("Helvetica",8,"bold"),
                         fg=WHITE, bg=BG, width=16, anchor="w").grid(
                         row=row_i+1, column=0, padx=8, pady=5, sticky="w")
                for ci, plan in enumerate(plans):
                    var_key = f"feat_{fk}"
                    tk.Checkbutton(inner, variable=sv[plan][var_key],
                                   bg=BG, fg=plan_cols[plan],
                                   selectcolor=BG_INPUT,
                                   activebackground=BG,
                                   text="On").grid(
                                   row=row_i+1, column=ci+1, padx=6, pady=4)

        def _build_limits_tab():
            """Per-feature max uses and reset days."""
            canvas = tk.Canvas(content_frame, bg=BG, highlightthickness=0)
            vsb    = ttk.Scrollbar(content_frame, orient="vertical", command=canvas.yview)
            canvas.configure(yscrollcommand=vsb.set)
            vsb.pack(side="right", fill="y")
            canvas.pack(side="left", fill="both", expand=True)
            inner  = tk.Frame(canvas, bg=BG)
            cw     = canvas.create_window((0,0), window=inner, anchor="nw")
            inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.bind("<Configure>", lambda e: canvas.itemconfig(cw, width=e.width))

            tk.Label(inner, text="Feature", font=("Helvetica",9,"bold"),
                     fg=WHITE, bg=BG, width=14, anchor="w").grid(row=0, column=0, padx=6, pady=4)
            for ci, plan in enumerate(plans):
                tk.Label(inner, text=plan, font=("Helvetica",9,"bold"),
                         fg=plan_cols[plan], bg=BG_CARD, padx=12, pady=4).grid(
                         row=0, column=ci+1, padx=6, pady=4)

            for ri, feat in enumerate(FEATURES):
                tk.Frame(inner, bg=BORDER, height=1).grid(
                    row=ri*2+1, column=0, columnspan=5, sticky="ew", padx=4)
                lbl = {"batch":"📦 Batch","single":"⬇ Single","audio":"🎵 Audio",
                       "convert":"🔄 Convert","subtitles":"📄 Subtitles",
                       "metadata":"ℹ Metadata","thumbnail":"🖼 Thumbnail"}.get(feat, feat)
                tk.Label(inner, text=lbl, font=("Helvetica",9,"bold"),
                         fg=WHITE, bg=BG, anchor="w", width=14).grid(
                         row=ri*2+2, column=0, padx=6, pady=6, sticky="w")
                for ci, plan in enumerate(plans):
                    mx_var, rd_var = lv[plan][feat]
                    cell = tk.Frame(inner, bg=BG_CARD, padx=8, pady=6)
                    cell.grid(row=ri*2+2, column=ci+1, padx=6, pady=4)
                    tk.Label(cell, text="Max Uses:", font=("Helvetica",7), fg=MUTED, bg=BG_CARD).grid(row=0, column=0, sticky="w")
                    tk.Entry(cell, textvariable=mx_var, width=6, bg=BG_INPUT, fg=WHITE,
                             font=("Helvetica",9,"bold"), relief="flat",
                             insertbackground=WHITE).grid(row=0, column=1, padx=(4,0))
                    tk.Label(cell, text="Reset days:", font=("Helvetica",7), fg=MUTED, bg=BG_CARD).grid(row=1, column=0, sticky="w", pady=(4,0))
                    tk.Entry(cell, textvariable=rd_var, width=6, bg=BG_INPUT, fg=WHITE,
                             font=("Helvetica",9,"bold"), relief="flat",
                             insertbackground=WHITE).grid(row=1, column=1, padx=(4,0), pady=(4,0))

        # ── Tab buttons ───────────────────────────────────────────────────
        def _switch(tab, btn_s, btn_l):
            tab_var.set(tab)
            btn_s.config(bg=ACCENT if tab=="settings" else BG_CARD,
                         fg=WHITE if tab=="settings" else MUTED)
            btn_l.config(bg=ACCENT if tab=="limits" else BG_CARD,
                         fg=WHITE if tab=="limits" else MUTED)
            show_tab(tab)

        btn_s = tk.Button(tab_bar, text="⚙ Plan Settings",
                          font=("Helvetica",9,"bold"), bg=ACCENT, fg=WHITE,
                          relief="flat", bd=0, padx=16, pady=6, cursor="hand2")
        btn_l = tk.Button(tab_bar, text="📊 Usage Limits",
                          font=("Helvetica",9,"bold"), bg=BG_CARD, fg=MUTED,
                          relief="flat", bd=0, padx=16, pady=6, cursor="hand2")
        btn_s.config(command=lambda: _switch("settings", btn_s, btn_l))
        btn_l.config(command=lambda: _switch("limits",   btn_s, btn_l))
        btn_s.pack(side="left", padx=(8,2), pady=4)
        btn_l.pack(side="left", padx=2, pady=4)

        show_tab("settings")

        def _save():
            try:
                conn = _auth_conn()
                _ensure_limits_table(conn)
                for plan in plans:
                    # Save plan_settings
                    conn.execute("""
                        INSERT INTO plan_settings(
                            plan_type,display_name,price,orig_price,validity_days,badge,enabled,
                            feat_batch,feat_single,feat_audio,feat_convert,feat_subtitles,
                            feat_metadata,feat_thumbnail,feat_4k,feat_8k,max_res,extra_notes)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(plan_type) DO UPDATE SET
                            display_name=excluded.display_name,
                            price=excluded.price,
                            orig_price=excluded.orig_price,
                            validity_days=excluded.validity_days,
                            badge=excluded.badge,
                            enabled=excluded.enabled,
                            feat_batch=excluded.feat_batch,
                            feat_single=excluded.feat_single,
                            feat_audio=excluded.feat_audio,
                            feat_convert=excluded.feat_convert,
                            feat_subtitles=excluded.feat_subtitles,
                            feat_metadata=excluded.feat_metadata,
                            feat_thumbnail=excluded.feat_thumbnail,
                            feat_4k=excluded.feat_4k,
                            feat_8k=excluded.feat_8k,
                            max_res=excluded.max_res,
                            extra_notes=excluded.extra_notes
                    """, (
                        plan,
                        sv[plan]["display_name"].get(),
                        int(sv[plan]["price"].get() or 0),
                        int(sv[plan]["orig_price"].get() or 0),
                        int(sv[plan]["validity_days"].get() or 0),
                        sv[plan]["badge"].get(),
                        int(sv[plan]["enabled"].get()),
                        int(sv[plan]["feat_batch"].get()),
                        int(sv[plan]["feat_single"].get()),
                        int(sv[plan]["feat_audio"].get()),
                        int(sv[plan]["feat_convert"].get()),
                        int(sv[plan]["feat_subtitles"].get()),
                        int(sv[plan]["feat_metadata"].get()),
                        int(sv[plan]["feat_thumbnail"].get()),
                        int(sv[plan]["feat_4k"].get()),
                        int(sv[plan]["feat_8k"].get()),
                        int(sv[plan]["max_res"].get() or 720),
                        sv[plan]["extra_notes"].get(),
                    ))
                    # Save feature limits
                    for feat in FEATURES:
                        mx_var, rd_var = lv[plan][feat]
                        conn.execute("""
                            INSERT INTO plan_feature_limits(plan_type,feature,max_uses,reset_days)
                            VALUES(?,?,?,?)
                            ON CONFLICT(plan_type,feature) DO UPDATE
                            SET max_uses=excluded.max_uses, reset_days=excluded.reset_days
                        """, (plan, feat, int(mx_var.get() or 1), int(rd_var.get() or 7)))
                conn.commit(); conn.close()
                if st_lbl.winfo_exists():
                    st_lbl.config(text="✅ All settings saved!", fg=GREEN)
                    win.after(2500, lambda: st_lbl.config(text="")
                              if st_lbl.winfo_exists() else None)
            except ValueError as e:
                if st_lbl.winfo_exists():
                    st_lbl.config(text=f"❌ Invalid value: {e}", fg=ACCENT)
            except Exception as e:
                if st_lbl.winfo_exists():
                    st_lbl.config(text=f"❌ {e}", fg=ACCENT)

        # ── Reset Defaults helper (for Usage Limits tab) ──────────────────
        def _reset_defaults():
            for plan in plans:
                for feat in FEATURES:
                    if feat in lv[plan]:
                        idx = FEATURES.index(feat)
                        lv[plan][feat][0].set(str(_DEFAULT_LIMITS[plan][idx]))
                        rd_val = (7 if plan in ("FREE","DAILY")
                                  else 30 if plan == "MONTHLY" else 365)
                        lv[plan][feat][1].set(str(rd_val))
            if st_lbl.winfo_exists():
                st_lbl.config(text="↩ Defaults restored — Save dabao!", fg=YELLOW)
                win.after(2500, lambda: st_lbl.config(text="")
                          if st_lbl.winfo_exists() else None)

        # ── Bottom bar ────────────────────────────────────────────────────
        tk.Frame(win, bg=BORDER, height=1).pack(fill="x", side="bottom")
        btm = tk.Frame(win, bg=BG, pady=8)
        btm.pack(fill="x", side="bottom", padx=16)

        st_lbl = tk.Label(btm, text="", font=("Helvetica",9,"bold"),
                          fg=GREEN, bg=BG, anchor="w")
        st_lbl.pack(side="left")

        tk.Button(btm, text="Close", font=("Helvetica",9),
                  bg="#2a0a0a", fg=MUTED, relief="flat", bd=0,
                  padx=14, pady=6, cursor="hand2",
                  command=win.destroy).pack(side="right", padx=(6,0))
        tk.Button(btm, text="💾 Save All Changes",
                  font=("Helvetica",10,"bold"), bg=GREEN, fg=BG,
                  relief="flat", bd=0, padx=18, pady=6,
                  cursor="hand2", command=_save).pack(side="right")
        tk.Button(btm, text="↩ Reset to Defaults",
                  font=("Helvetica",9), bg=BG_CARD, fg=MUTED,
                  relief="flat", bd=0, padx=14, pady=6,
                  cursor="hand2", command=_reset_defaults).pack(side="right", padx=(0,6))

    def _change_pw(self):
        win = tk.Toplevel(self)
        win.title("Change Admin Password")
        win.geometry("360x290")
        win.resizable(False,False)
        win.configure(bg=BG)
        win.grab_set()
        win.update_idletasks()
        sw,sh = win.winfo_screenwidth(), win.winfo_screenheight()
        x = str((sw-360)//2)
        y = str((sh-290)//2)
        win.geometry("360x290+"+x+"+"+y)
        tk.Label(win, text="Change Admin Password",
                 font=("Helvetica",12,"bold"),
                 fg=WHITE, bg=BG).pack(pady=(20,14))
        frm = tk.Frame(win, bg=BG, padx=30)
        frm.pack(fill="x")

        def _ent(lbl):
            tk.Label(frm, text=lbl, font=("Helvetica",8,"bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            v = tk.StringVar()
            tk.Entry(frm, textvariable=v, show="*",
                     font=("Helvetica",10), bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE, relief="flat", bd=0
                     ).pack(fill="x", ipady=7, pady=(2,10))
            return v

        cur_v  = _ent("Current Password")
        new_v  = _ent("New Password")
        cnew_v = _ent("Confirm New Password")
        st = tk.Label(frm, text="", font=("Helvetica",8),
                       fg=ACCENT, bg=BG)
        st.pack()

        def _save():
            if not self._admin_id: return
            conn = _auth_conn()
            row  = conn.execute("SELECT email FROM admins WHERE id=?",
                                (self._admin_id,)).fetchone()
            conn.close()
            if not row: return
            if not self._verify_admin(row[0], cur_v.get()):
                st.config(text="Current password galat."); return
            if len(new_v.get()) < 6:
                st.config(text="Min 6 characters."); return
            if new_v.get() != cnew_v.get():
                st.config(text="Passwords match nahi."); return
            conn = _auth_conn()
            conn.execute("UPDATE admins SET password_hash=? WHERE id=?",
                         (_hash_pw(new_v.get()), self._admin_id))
            conn.commit(); conn.close()
            st.config(text="Password change ho gaya!", fg=GREEN)
            win.after(1500, win.destroy)

        tk.Button(frm, text="Save New Password",
                  font=("Helvetica",10,"bold"),
                  bg=GREEN, fg=BG, relief="flat", bd=0,
                  pady=9, cursor="hand2",
                  command=_save).pack(fill="x", pady=(4,0))


class UPIPaymentDialog(tk.Toplevel):
    """
    UPI payment flow — v3 (FIXED layout):
    • Top section   : scrollable canvas  (header, amount, UPI ID, steps, UTR input)
    • Bottom section: FIXED buttons bar  (Submit + Cancel always visible, no scroll needed)
    """
    def __init__(self, master, plan_key, plan_name, price, on_paid=None):
        super().__init__(master)
        self._plan_key  = plan_key
        self._plan_name = plan_name
        self._price     = price
        self._on_paid   = on_paid
        self.title(f"Pay ₹{price} — {plan_name} Plan")
        self.geometry("480x600")
        self.minsize(420, 460)
        self.resizable(True, True)
        self.configure(bg=BG)
        self.grab_set()
        self._build()
        self._center()

    def _center(self):
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        h = min(h, sh - 60)
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _build(self):
        # ════════════════════════════════════════════════════════════════════
        # STRUCTURE:
        #   self (Toplevel)
        #    ├── scroll_area  (fill+expand)  ← canvas + scrollbar
        #    └── fixed_bottom (fill="x")     ← Submit + Cancel ALWAYS visible
        # ════════════════════════════════════════════════════════════════════

        # ── FIXED BOTTOM BAR — pack FIRST so it stays pinned ────────────────
        fixed_bottom = tk.Frame(self, bg=BG, pady=10, padx=20)
        fixed_bottom.pack(side="bottom", fill="x")

        # Separator above buttons
        tk.Frame(fixed_bottom, bg=BORDER, height=1).pack(fill="x", pady=(0, 10))

        self._st = tk.Label(fixed_bottom, text="",
                             font=("Helvetica", 8),
                             fg=ACCENT, bg=BG, wraplength=420)
        self._st.pack(fill="x", pady=(0, 8))

        self._sub_btn = tk.Button(fixed_bottom,
            text=f"✅  Submit Payment  —  ₹{self._price}",
            font=("Helvetica", 12, "bold"),
            bg=GREEN, fg="#000000", relief="flat", bd=0,
            pady=13, cursor="hand2",
            command=self._submit)
        self._sub_btn.pack(fill="x", pady=(0, 6))

        tk.Button(fixed_bottom, text="✖  Cancel",
            font=("Helvetica", 10, "bold"),
            bg="#2a0a0a", fg=ACCENT, relief="flat", bd=0,
            pady=9, cursor="hand2",
            command=self.destroy).pack(fill="x")

        # ── SCROLLABLE TOP AREA — pack after bottom so it fills remaining space
        scroll_wrap = tk.Frame(self, bg=BG)
        scroll_wrap.pack(side="top", fill="both", expand=True)

        canvas = tk.Canvas(scroll_wrap, bg=BG, highlightthickness=0)
        vsb    = ttk.Scrollbar(scroll_wrap, orient="vertical",
                               command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=BG)
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _resize_inner(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def _resize_canvas(e):
            canvas.itemconfig(win_id, width=e.width)
        inner.bind("<Configure>", _resize_inner)
        canvas.bind("<Configure>", _resize_canvas)

        def _wheel(e):
            canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _wheel)

        # ── HEADER ───────────────────────────────────────────────────────────
        hdr = tk.Frame(inner, bg=BG, pady=14)
        hdr.pack(fill="x")
        tk.Label(hdr, text="💳  UPI Payment",
                 font=("Helvetica", 16, "bold"),
                 fg=WHITE, bg=BG).pack()
        tk.Label(hdr, text="Google Pay  ·  PhonePe  ·  Paytm  ·  Any UPI App",
                 font=("Helvetica", 8), fg=MUTED, bg=BG).pack(pady=(2, 0))

        tk.Frame(inner, bg=BORDER, height=1).pack(fill="x")

        # ── AMOUNT CARD ───────────────────────────────────────────────────────
        amt = tk.Frame(inner, bg=BG_CARD, padx=20, pady=14)
        amt.pack(fill="x", padx=20, pady=(14, 0))
        tk.Label(amt, text=f"{self._plan_name} Plan",
                 font=("Helvetica", 10, "bold"),
                 fg=MUTED, bg=BG_CARD).pack()
        tk.Label(amt, text=f"₹{self._price}",
                 font=("Helvetica", 34, "bold"),
                 fg=GREEN, bg=BG_CARD).pack()
        tk.Label(amt, text="Bilkul yahi amount bhejo — kam/zyada mat karo",
                 font=("Helvetica", 8), fg=MUTED, bg=BG_CARD).pack()

        # ── UPI ID SECTION ────────────────────────────────────────────────────
        upi_f = tk.Frame(inner, bg=BG_CARD, padx=20, pady=12)
        upi_f.pack(fill="x", padx=20, pady=(10, 0))

        tk.Label(upi_f, text="Pay to UPI ID:",
                 font=("Helvetica", 8, "bold"),
                 fg=MUTED, bg=BG_CARD).pack(anchor="w")

        upi_row = tk.Frame(upi_f, bg=BG_INPUT,
                           highlightbackground=YELLOW,
                           highlightthickness=1)
        upi_row.pack(fill="x", pady=(6, 0))

        tk.Label(upi_row, text=ADMIN_UPI_ID,
                 font=("Helvetica", 13, "bold"),
                 fg=YELLOW, bg=BG_INPUT,
                 padx=12, pady=10).pack(side="left", fill="x", expand=True)

        def _copy_upi():
            self.clipboard_clear()
            self.clipboard_append(ADMIN_UPI_ID)
            if copy_btn.winfo_exists():
                copy_btn.config(text="✅ Copied!", fg=GREEN)
            self.after(2000, lambda: copy_btn.config(
                text="📋 Copy", fg=MUTED) if copy_btn.winfo_exists() else None)

        copy_btn = tk.Button(upi_row, text="📋 Copy",
            font=("Helvetica", 8, "bold"),
            bg=BG_INPUT, fg=MUTED, relief="flat", bd=0,
            padx=12, cursor="hand2", command=_copy_upi)
        copy_btn.pack(side="right")

        tk.Label(upi_f,
                 text=f"Name: {ADMIN_UPI_NAME}",
                 font=("Helvetica", 8), fg=MUTED, bg=BG_CARD
                 ).pack(anchor="w", pady=(8, 0))

        # ── STEPS ─────────────────────────────────────────────────────────────
        steps_f = tk.Frame(inner, bg=BG_INPUT, padx=16, pady=10)
        steps_f.pack(fill="x", padx=20, pady=(12, 0))

        for num, step_txt in [
            ("1", "Google Pay / PhonePe / Paytm app kholo"),
            ("2", f"UPI ID  {ADMIN_UPI_ID}  pe  ₹{self._price}  bhejo"),
            ("3", "Payment hone ke baad Transaction ID / UTR copy karo"),
            ("4", "Neeche wale box mein paste karo  →  Submit dabao"),
        ]:
            row = tk.Frame(steps_f, bg=BG_INPUT)
            row.pack(fill="x", pady=4)
            tk.Label(row, text=num,
                     font=("Helvetica", 9, "bold"),
                     bg=ACCENT, fg=WHITE,
                     width=2, pady=3).pack(side="left", padx=(0, 10))
            tk.Label(row, text=step_txt,
                     font=("Helvetica", 9),
                     bg=BG_INPUT, fg=WHITE,
                     anchor="w").pack(side="left", fill="x", expand=True)

        # ── UTR INPUT ─────────────────────────────────────────────────────────
        inp_f = tk.Frame(inner, bg=BG, padx=20)
        inp_f.pack(fill="x", pady=(14, 4))

        tk.Label(inp_f, text="Transaction ID / UTR Number:",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG).pack(anchor="w")

        utr_wrap = tk.Frame(inp_f, bg=BG_INPUT,
                            highlightbackground=ACCENT,
                            highlightthickness=1)
        utr_wrap.pack(fill="x", pady=(6, 0))
        self._utr_var = tk.StringVar()
        tk.Entry(utr_wrap, textvariable=self._utr_var,
                 font=("Helvetica", 13), bg=BG_INPUT, fg=WHITE,
                 insertbackground=WHITE, relief="flat", bd=0
                 ).pack(fill="x", ipady=11, padx=10)

        tk.Label(inner,
                 text="⏱  Plan admin verify ke baad activate hoti hai  (usually 1 ghante mein)",
                 font=("Helvetica", 7), fg=DIM, bg=BG,
                 wraplength=420).pack(pady=(10, 16))

    def _submit(self):
        utr = self._utr_var.get().strip()
        if not utr:
            self._st.config(
                text="Transaction ID daalo.", fg=ACCENT)
            return
        if len(utr) < 6:
            self._st.config(
                text="Valid Transaction ID daalo (min 6 chars).", fg=ACCENT)
            return

        self._sub_btn.config(state="disabled", text="Submitting…")

        name  = _S.get("name", "")
        email = _S.get("email", "")
        now   = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Save to DB for admin to review
        try:
            conn = _auth_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS upi_payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_email TEXT, user_name TEXT,
                    plan TEXT, price INTEGER, utr TEXT,
                    status TEXT DEFAULT 'pending',
                    submitted_at TEXT DEFAULT (datetime('now'))
                )""")
            conn.execute(
                "INSERT INTO upi_payments"
                "(user_email,user_name,plan,price,utr,status)"
                " VALUES(?,?,?,?,?,'pending')",
                (email, name, self._plan_key, self._price, utr))
            conn.commit(); conn.close()
        except Exception:
            pass

        # Email admin
        body = (
            f"NEW UPI PAYMENT SUBMITTED\n"
            f"{'='*40}\n"
            f"User  : {name}\n"
            f"Email : {email}\n"
            f"Plan  : {self._plan_name}  (Rs.{self._price})\n"
            f"UTR   : {utr}\n"
            f"Time  : {now}\n"
            f"{'='*40}\n"
            f"Open Admin Panel to approve/reject.\n"
        )
        threading.Thread(target=_send_email,
            args=(ADMIN_EMAIL,
                  f"[FastTube Pro] UPI Payment — {name} Rs.{self._price}",
                  body), daemon=True).start()

        # ✅ DO NOT activate here — subscription activates only when admin approves
        # sub_activate(self._plan_key)  ← removed intentionally

        self._st.config(
            text="⏳ Payment submitted & pending admin approval.\n"
                 "Your plan will activate once admin verifies your UTR.",
            fg=YELLOW)
        self._sub_btn.config(text="✅ Done!")
        self.after(2500, self._done)

    def _done(self):
        self.destroy()
        if self._on_paid:
            self._on_paid(self._plan_key)

# ══════════════════════════════════════════════════════════════════════════════
#  LIMIT POPUP  (shown when download blocked)
# ══════════════════════════════════════════════════════════════════════════════
def show_limit_popup(master, reason):
    win = tk.Toplevel(master)
    win.title("Limit Reached")
    win.geometry("360x200")
    win.resizable(False,False)
    win.configure(bg=BG)
    win.grab_set()
    win.update_idletasks()
    sw,sh = win.winfo_screenwidth(), win.winfo_screenheight()
    win.geometry(f"360x200+{(sw-360)//2}+{(sh-200)//2}")

    tk.Label(win, text="🚫 Download Limit Reached",
        font=("Helvetica",12,"bold"), fg=ACCENT, bg=BG).pack(pady=(20,6))
    tk.Label(win, text=reason,
        font=("Helvetica",9), fg=WHITE, bg=BG,
        wraplength=320, justify="center").pack(pady=(0,14))

    row = tk.Frame(win,bg=BG); row.pack()
    def _up():
        win.destroy()
        UpgradeDialog(master)
    tk.Button(row, text="⚡ Upgrade Plan",
        font=("Helvetica",10,"bold"), bg=ACCENT, fg=WHITE,
        relief="flat", bd=0, padx=18, pady=8, cursor="hand2",
        command=_up).pack(side="left",padx=6)
    tk.Button(row, text="Cancel",
        font=("Helvetica",9), bg=DIM, fg=MUTED,
        relief="flat", bd=0, padx=12, pady=8, cursor="hand2",
        command=win.destroy).pack(side="left")
    win.wait_window()

# ══════════════════════════════════════════════════════════════════════════════
#  SHARED UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def make_scrollable(parent, bg=BG):
    canvas = tk.Canvas(parent, bg=bg, highlightthickness=0)
    vsb    = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    canvas.pack(side="left", fill="both", expand=True)
    vsb.pack(side="right", fill="y")
    inner = tk.Frame(canvas, bg=bg)
    iid   = canvas.create_window((0, 0), window=inner, anchor="nw")
    inner.bind("<Configure>",
               lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.bind("<Configure>",
                lambda e: canvas.itemconfig(iid, width=e.width))
    canvas.bind_all("<MouseWheel>",
                    lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))
    # Store canvas ref on inner for scroll buttons
    inner._canvas = canvas
    return inner

# ── Disclaimer text (shared across all tabs) ──────────────────────────────────
_DISCLAIMER_LINES = [
    ("⚠", "Temporary data issues may occasionally occur during downloads."),
    ("⏱", "Subscription approval may take up to 5 minutes after payment."),
    ("💬", "For any issues, contact helpline support: @amane_lv_mahiru"),
]

def add_disclaimer(parent_inner):
    """
    Add disclaimer card + scroll-to-top / scroll-to-bottom buttons
    at the bottom of any scrollable tab inner frame.
    """
    canvas = getattr(parent_inner, "_canvas", None)

    # ── Disclaimer card ────────────────────────────────────────────────────
    disc = tk.Frame(parent_inner, bg="#0e1520", padx=14, pady=10,
                    highlightbackground="#1e3050", highlightthickness=1)
    disc.pack(fill="x", padx=14, pady=(8, 4))

    tk.Label(disc, text="📋  Important Notice",
             font=("Helvetica", 8, "bold"), fg="#5a9fd4", bg="#0e1520").pack(anchor="w", pady=(0, 5))

    for icon, text in _DISCLAIMER_LINES:
        row = tk.Frame(disc, bg="#0e1520")
        row.pack(fill="x", pady=2)
        tk.Label(row, text=f"  {icon}",
                 font=("Helvetica", 9), fg="#5a9fd4", bg="#0e1520",
                 width=3).pack(side="left")
        tk.Label(row, text=text,
                 font=("Helvetica", 8), fg="#8ab4d4", bg="#0e1520",
                 anchor="w", wraplength=700, justify="left").pack(
                 side="left", fill="x", expand=True)

    # ── Scroll nav buttons ─────────────────────────────────────────────────
    nav = tk.Frame(parent_inner, bg=BG)
    nav.pack(fill="x", padx=14, pady=(2, 10))

    def scroll_top():
        if canvas:
            canvas.yview_moveto(0)

    def scroll_bottom():
        if canvas:
            canvas.yview_moveto(1)

    nav_btn_cfg = dict(font=("Helvetica", 8, "bold"), relief="flat", bd=0,
                       padx=14, pady=5, cursor="hand2")

    tk.Button(nav, text="⬆  Top", bg="#1a1a2e", fg="#5a9fd4",
              command=scroll_top, **nav_btn_cfg).pack(side="left", padx=(0, 6))
    tk.Button(nav, text="⬇  Bottom", bg="#1a1a2e", fg="#5a9fd4",
              command=scroll_bottom, **nav_btn_cfg).pack(side="left")

    # Also add floating scroll buttons on canvas (top-right corner)
    if canvas:
        def _place_float(event=None):
            cw = canvas.winfo_width()
            ch = canvas.winfo_height()
            # Top button
            canvas.delete("scroll_btn_tag")
            # We use window items for crisp rendering
            pass  # handled via nav bar above



def CardFrame(parent, **kw):
    d = dict(bg=BG_CARD, padx=16, pady=12)
    d.update(kw)
    return tk.Frame(parent, **d)

def Hdr(parent, text, fg=MUTED, fs=9, bold=True):
    return tk.Label(parent, text=text,
                    font=("Helvetica", fs, "bold" if bold else "normal"),
                    fg=fg, bg=parent["bg"])

def Btn(parent, text, cmd, bg=ACCENT, fg=WHITE, fs=10, py=6, px=14):
    orig = bg
    b = tk.Button(parent, text=text, command=cmd,
                  font=("Helvetica", fs, "bold"),
                  bg=bg, fg=fg,
                  activebackground=AHOVER if bg == ACCENT else "#4a4a4a",
                  activeforeground=WHITE,
                  relief="flat", bd=0, padx=px, pady=py, cursor="hand2")
    b.bind("<Enter>", lambda e, w=b, c=orig:
           w.config(bg=AHOVER if c == ACCENT else "#4a4a4a"))
    b.bind("<Leave>", lambda e, w=b, c=orig: w.config(bg=c))
    return b

def Inp(parent, var=None, w=0, **kw):
    d = dict(font=("Helvetica",10), bg=BG_INPUT, fg=WHITE,
             insertbackground=WHITE, relief="flat", bd=0,
             highlightthickness=1, highlightbackground=BORDER,
             highlightcolor=ACCENT)
    d.update(kw)
    if var: d["textvariable"] = var
    if w:   d["width"] = w
    return tk.Entry(parent, **d)

def mk_pb(parent):
    s = ttk.Style()
    s.theme_use("clam")
    s.configure("FT.Horizontal.TProgressbar",
                troughcolor=BG_INPUT, background=ACCENT,
                bordercolor=BG, lightcolor=ACCENT, darkcolor=ACCENT)
    return ttk.Progressbar(parent, style="FT.Horizontal.TProgressbar",
                           orient="horizontal", mode="determinate")

def mk_combo(parent, var, values, w=16):
    s = ttk.Style()
    s.configure("FT.TCombobox",
                fieldbackground=BG_INPUT, background=BG_INPUT,
                foreground=WHITE, bordercolor=BORDER,
                arrowcolor=MUTED,
                selectbackground=ACCENT, selectforeground=WHITE)
    return ttk.Combobox(parent, textvariable=var, values=values,
                        state="readonly", font=("Helvetica",10),
                        width=w, style="FT.TCombobox")

def tv_style():
    s = ttk.Style()
    s.configure("Treeview",
                background=BG_CARD, foreground=WHITE,
                fieldbackground=BG_CARD, rowheight=22)
    s.configure("Treeview.Heading",
                background=BG_INPUT, foreground=MUTED,
                font=("Helvetica", 8, "bold"))
    s.map("Treeview",
          background=[("selected", ACCENT)],
          foreground=[("selected", WHITE)])

def sep(parent):
    tk.Frame(parent, bg=BORDER, height=1).pack(fill="x", padx=14, pady=6)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — BATCH DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — BATCH DOWNLOADER
#  Flow: Paste playlist/multiple URLs → Fetch → Quality select → Download All
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — BATCH DOWNLOADER
#  Naya Flow:
#    1. Playlist URL ya multiple URLs paste karo
#    2. "Fetch" button dabao → saare videos list mein dikhenge
#    3. Quality manually choose karo (dropdown)
#    4. "Download All" dabao → sab download hoga
#  Features: Pause/Resume/Cancel/Retry per job, Pause All/Resume All, History
# ══════════════════════════════════════════════════════════════════════════════
class BatchTab(tk.Frame):

    # ── Inner Job Row ─────────────────────────────────────────────────────────
    class _Job:
        def __init__(self, container, idx, url, title, quality, save_dir):
            self.url       = url
            self.quality   = quality
            self.save_dir  = save_dir
            self.state     = "queued"
            self._pev      = threading.Event()
            self._pev.set()
            self._cancel   = False
            self.title     = sanitize(title or f"video_{idx+1}")
            self.pkey      = detect_platform(url)

            pinfo = PLATFORMS.get(self.pkey, {})
            col   = pinfo.get("color", MUTED)
            icon  = pinfo.get("icon", "🌐")

            # Frame
            self.frame = tk.Frame(container, bg=BG_CARD, pady=6, padx=10)
            self.frame.pack(fill="x", pady=(0, 2))

            # Row 1: number + title + status
            r1 = tk.Frame(self.frame, bg=BG_CARD)
            r1.pack(fill="x")
            tk.Label(r1, text=f"{icon} #{idx+1}",
                     font=("Helvetica", 8, "bold"),
                     fg=col, bg=BG_CARD).pack(side="left")
            disp = (self.title[:62] + "…") if len(self.title) > 62 else self.title
            tk.Label(r1, text=disp,
                     font=("Helvetica", 8), fg=WHITE,
                     bg=BG_CARD, anchor="w").pack(
                     side="left", padx=(6, 0), fill="x", expand=True)
            self.st_lbl = tk.Label(r1, text="⏳ Queued",
                                   font=("Helvetica", 8, "bold"),
                                   fg=YELLOW, bg=BG_CARD)
            self.st_lbl.pack(side="right")

            # Row 2: speed + eta + size
            r2 = tk.Frame(self.frame, bg=BG_CARD)
            r2.pack(fill="x")
            self.spd_lbl = tk.Label(r2, text="",
                                    font=("Helvetica", 8), fg=GREEN, bg=BG_CARD)
            self.spd_lbl.pack(side="left")
            self.eta_lbl = tk.Label(r2, text="",
                                    font=("Helvetica", 8), fg=YELLOW, bg=BG_CARD)
            self.eta_lbl.pack(side="left", padx=(8, 0))
            self.sz_lbl  = tk.Label(r2, text="",
                                    font=("Helvetica", 8), fg=MUTED, bg=BG_CARD)
            self.sz_lbl.pack(side="right")

            # Progress bar
            self.pb = mk_pb(self.frame)
            self.pb.pack(fill="x", pady=(3, 2))

            # Row 3: percent + buttons
            r3 = tk.Frame(self.frame, bg=BG_CARD)
            r3.pack(fill="x")
            self.pct_lbl = tk.Label(r3, text="0%",
                                    font=("Helvetica", 8, "bold"),
                                    fg=WHITE, bg=BG_CARD)
            self.pct_lbl.pack(side="left")

            self.retry_btn = Btn(r3, "🔄 Retry", self._retry,
                                 bg=BG_INPUT, fg=MUTED, fs=7, py=2, px=6)
            self.retry_btn.pack(side="right", padx=(3, 0))
            self.cancel_btn = Btn(r3, "✖ Cancel", self._do_cancel,
                                  bg=DIM, fg=MUTED, fs=7, py=2, px=6)
            self.cancel_btn.pack(side="right", padx=(3, 0))
            self.pause_btn = Btn(r3, "⏸ Pause", self._toggle_pause,
                                 bg=DIM, fg=WHITE, fs=7, py=2, px=6)
            self.pause_btn.pack(side="right", padx=(3, 0))

            self.pause_btn.config(state="disabled")
            self.cancel_btn.config(state="disabled")
            self.retry_btn.config(state="disabled")

            tk.Frame(self.frame, bg=BORDER, height=1).pack(fill="x", pady=(5, 0))

        def _set_st(self, txt, c=MUTED):
            self.st_lbl.config(text=txt, fg=c)

        def _upd(self, pct, dl, tot, spd, eta):
            self.pb.config(value=pct)
            self.pct_lbl.config(text=f"{pct:.0f}%")
            if spd:
                self.spd_lbl.config(text=f"⚡ {strip_ansi(spd)}")
            if eta:
                self.eta_lbl.config(text=f"ETA: {strip_ansi(eta)}")
            if tot > 0:
                ts = f"{tot/1024:.2f}GB" if tot >= 1024 else f"{tot:.1f}MB"
                ds = f"{dl/1024:.2f}GB"  if dl  >= 1024 else f"{dl:.1f}MB"
                self.sz_lbl.config(text=f"{ds}/{ts}")

        def _toggle_pause(self):
            if self.state == "running":
                self._pev.clear()
                self.state = "paused"
                self.pause_btn.config(text="▶ Resume", bg=GREEN)
                self._set_st("⏸ Paused", YELLOW)
            elif self.state == "paused":
                self._pev.set()
                self.state = "running"
                self.pause_btn.config(text="⏸ Pause", bg=DIM)
                self._set_st("⬇ Downloading…", BLUE)

        def _do_cancel(self):
            self._cancel = True
            self._pev.set()
            self.state = "cancelled"
            self._set_st("✖ Cancelled", MUTED)
            self.pb.config(value=0)
            self.pause_btn.config(state="disabled")
            self.cancel_btn.config(state="disabled")
            self.retry_btn.config(state="normal")

        def _retry(self):
            if self.state in ("error", "cancelled", "done"):
                self._cancel = False
                self._pev.set()
                self.state = "queued"
                self.pb.config(value=0)
                self.pct_lbl.config(text="0%", fg=WHITE)
                self.spd_lbl.config(text="")
                self.eta_lbl.config(text="")
                self._set_st("⏳ Queued", YELLOW)
                self.retry_btn.config(state="disabled")
                self._kick()

        def start(self):
            self.state = "running"
            self.pause_btn.config(state="normal")
            self.cancel_btn.config(state="normal")
            self._set_st("⬇ Downloading…", BLUE)
            self._kick()

        def _kick(self):
            threading.Thread(target=self._run, daemon=True).start()

        def _run(self):
            url    = self.url
            pkey   = self.pkey
            max_ci = len(PLAYER_CLIENTS) if pkey == "youtube" else 2
            fmt    = quality_to_fmt(self.quality)
            pp     = ([{"key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3", "preferredquality": "320"}]
                      if self.quality == "audio_mp3" else [])

            # FIX WinError 32: each job gets its own isolated temp folder
            # so FFmpeg never conflicts with other simultaneous downloads
            job_tmp = os.path.join(
                self.save_dir, f"_tmp_{self.title[:30]}_{id(self)}")
            os.makedirs(job_tmp, exist_ok=True)
            out = os.path.join(job_tmp, f"{self.title}.%(ext)s")

            def hook(d):
                if self._cancel:
                    raise Exception("__CANCELLED__")
                if not self._pev.is_set():
                    self._pev.wait()
                    if self._cancel:
                        raise Exception("__CANCELLED__")
                if d["status"] == "downloading":
                    tb  = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    db  = d.get("downloaded_bytes", 0)
                    pct = (db / tb * 100) if tb else 0
                    self.frame.after(0, lambda p=pct,
                        dm=to_mb(db), tm=to_mb(tb),
                        sp=d.get("_speed_str", "").strip(),
                        et=d.get("_eta_str", "").strip():
                        self._upd(p, dm, tm, sp, et))
                elif d["status"] == "finished":
                    self.frame.after(0,
                        lambda: self._set_st("⚙ Moving…", YELLOW))

            last_err = ""
            for ci in range(max_ci):
                if self._cancel:
                    break
                try:
                    opts = base_ydl_opts(pkey, ci)
                    opts.update({
                        "format":              fmt,
                        "outtmpl":             out,
                        "progress_hooks":      [hook],
                        "merge_output_format": "mp4",
                        "postprocessors":      pp,
                        "noplaylist":          True,
                        # Keep original files so FFmpeg can merge properly
                        "keepvideo":           False,
                        "writethumbnail":      False,
                    })
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        ydl.download([url])

                    # Move finished file from temp folder → save_dir
                    # WinError 32 fix: wait briefly for OS to release file lock
                    final_path = ""
                    for attempt in range(5):
                        try:
                            for fname in os.listdir(job_tmp):
                                if fname.startswith(".") or fname.endswith(".part"):
                                    continue
                                src_f = os.path.join(job_tmp, fname)
                                dst_f = os.path.join(self.save_dir, fname)
                                # If destination exists, add suffix
                                if os.path.exists(dst_f):
                                    base, ext = os.path.splitext(fname)
                                    dst_f = os.path.join(
                                        self.save_dir, f"{base}_{id(self)}{ext}")
                                shutil.move(src_f, dst_f)
                                final_path = dst_f
                            break  # move succeeded
                        except OSError as move_err:
                            if attempt < 4:
                                time.sleep(1.5)  # wait for file lock release
                            else:
                                raise move_err

                    # Cleanup temp dir
                    try:
                        shutil.rmtree(job_tmp, ignore_errors=True)
                    except Exception:
                        pass

                    self.state = "done"
                    self._final_path = final_path   # store for smart rename
                    self.frame.after(0, lambda fp=final_path: (
                        self._set_st("✅ Done", GREEN),
                        self.pb.config(value=100),
                        self.pct_lbl.config(text="100%", fg=GREEN),
                        self.pause_btn.config(state="disabled"),
                        self.cancel_btn.config(state="disabled"),
                        self.retry_btn.config(state="normal"),
                        self.sz_lbl.config(
                            text=f"{os.path.getsize(fp)/1024/1024:.1f}MB"
                            if fp and os.path.exists(fp) else ""),
                    ))
                    db_insert(url, self.title, pkey, self.quality,
                              final_path, "done")
                    sub_record_dl()
                    # NOTE: batch feature usage counted once per session in _start_all
                    return

                except Exception as e:
                    err = strip_ansi(str(e))
                    if "__CANCELLED__" in err or self._cancel:
                        self.state = "cancelled"
                        self.frame.after(0,
                            lambda: self._set_st("✖ Cancelled", MUTED))
                        db_insert(url, self.title, pkey,
                                  self.quality, "", "cancelled")
                        # Cleanup temp
                        try:
                            shutil.rmtree(job_tmp, ignore_errors=True)
                        except Exception:
                            pass
                        return
                    last_err = err
                    # WinError 32 — wait and retry same client
                    if "winerror 32" in err.lower() or "cannot access" in err.lower():
                        self.frame.after(0, lambda:
                            self._set_st("⏳ File busy, retrying…", YELLOW))
                        time.sleep(2)
                        continue
                    if any(k in err.lower() for k in
                           ["player", "bot", "jsinterp", "403",
                            "sign in", "login"]):
                        continue
                    break

            # All retries failed — cleanup temp
            try:
                shutil.rmtree(job_tmp, ignore_errors=True)
            except Exception:
                pass

            self.state = "error"
            msg = strip_ansi(last_err)[:72]
            self.frame.after(0, lambda m=msg: (
                self._set_st(f"❌ {m}", ACCENT),
                self.pause_btn.config(state="disabled"),
                self.cancel_btn.config(state="disabled"),
                self.retry_btn.config(state="normal"),
            ))
            db_insert(url, self.title, pkey, self.quality, "", "error")

    # ── BatchTab UI ───────────────────────────────────────────────────────────
    def __init__(self, parent):
        super().__init__(parent, bg=BG)
        self.jobs         = []
        self.save_dir     = get_save_dir()
        self._fetched     = []   # [(url, title, duration_str), …]
        self._last_states = {}
        self._queue_idx   = 0
        self._cancel_seq  = False   # stops sequential queue
        self._build()

    def _build(self):
        # ══════════════════════════════════════════════════════════════════════
        #  LAYOUT STRATEGY:
        #   self (BatchTab Frame) uses grid with 3 rows:
        #     row 0  weight=0  → top controls (fixed ~320px scrollable area)
        #     row 1  weight=1  → bottom: job cards + queue log (expands)
        #     row 2  weight=0  → disclaimer strip (fixed, always visible)
        # ══════════════════════════════════════════════════════════════════════
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=0)   # controls - fixed
        self.rowconfigure(1, weight=1)   # job cards - expands
        self.rowconfigure(2, weight=0)   # disclaimer - fixed

        # ══════════════════════════════════════════════════════════════════════
        #  ROW 0 — Controls (scrollable, fixed height wrapper)
        # ══════════════════════════════════════════════════════════════════════
        ctrl_wrap = tk.Frame(self, bg=BG, height=310)
        ctrl_wrap.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 0))
        ctrl_wrap.pack_propagate(False)
        ctrl_wrap.grid_propagate(False)

        ctrl_canvas = tk.Canvas(ctrl_wrap, bg=BG, highlightthickness=0)
        ctrl_vsb = ttk.Scrollbar(ctrl_wrap, orient="vertical",
                                 command=ctrl_canvas.yview)
        ctrl_canvas.configure(yscrollcommand=ctrl_vsb.set)
        ctrl_vsb.pack(side="right", fill="y")
        ctrl_canvas.pack(side="left", fill="both", expand=True)

        top = tk.Frame(ctrl_canvas, bg=BG_CARD, padx=14, pady=10)
        cc_win = ctrl_canvas.create_window((0, 0), window=top, anchor="nw")
        top.bind("<Configure>",
            lambda e: ctrl_canvas.configure(
                scrollregion=ctrl_canvas.bbox("all")))
        ctrl_canvas.bind("<Configure>",
            lambda e: ctrl_canvas.itemconfig(cc_win, width=e.width))
        ctrl_canvas.bind("<Enter>",
            lambda e: ctrl_canvas.bind_all("<MouseWheel>",
                lambda ev: ctrl_canvas.yview_scroll(
                    int(-1*(ev.delta/120)), "units")))
        ctrl_canvas.bind("<Leave>",
            lambda e: ctrl_canvas.unbind_all("<MouseWheel>"))

        # ── Title ─────────────────────────────────────────────────────────────
        hdr_row = tk.Frame(top, bg=BG_CARD)
        hdr_row.pack(fill="x", pady=(0, 6))
        tk.Label(hdr_row, text="📦 Batch Downloader",
                 font=("Helvetica", 13, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(side="left")
        tk.Label(hdr_row,
                 text="  Playlist URL → Fetch → Quality → Download All",
                 font=("Helvetica", 8), fg=MUTED, bg=BG_CARD).pack(
                 side="left", pady=(4, 0))

        # ── STEP 1: URL input ─────────────────────────────────────────────────
        tk.Label(top,
                 text="📋  Step 1 — Playlist URL  ya  Multiple URLs (ek line ek URL):",
                 font=("Helvetica", 9, "bold"),
                 fg=MUTED, bg=BG_CARD).pack(anchor="w")

        self.url_box = scrolledtext.ScrolledText(
            top, height=3,
            bg=BG_INPUT, fg=WHITE,
            insertbackground=WHITE,
            font=("Helvetica", 10),
            relief="flat",
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=ACCENT)
        self.url_box.pack(fill="x", pady=(4, 6))

        # Fetch button row
        fr = tk.Frame(top, bg=BG_CARD)
        fr.pack(fill="x", pady=(0, 6))
        self.fetch_btn = Btn(fr, "🔍  Fetch Videos / Playlist",
                             self._do_fetch, bg=ACCENT, fg=WHITE, fs=10, py=7)
        self.fetch_btn.pack(side="left", fill="x", expand=True, padx=(0, 10))
        self.fetch_lbl = tk.Label(fr, text="",
                                  font=("Helvetica", 9, "bold"),
                                  fg=YELLOW, bg=BG_CARD)
        self.fetch_lbl.pack(side="left")

        # ── Video list (Treeview) ─────────────────────────────────────────────
        tk.Label(top, text="📺  Fetched Videos:",
                 font=("Helvetica", 9, "bold"),
                 fg=MUTED, bg=BG_CARD).pack(anchor="w")

        tv_style()
        vlist_frame = tk.Frame(top, bg=BG_CARD)
        vlist_frame.pack(fill="x", pady=(3, 6))

        cols   = ("#", "Title", "Duration", "URL")
        widths = [35, 430, 80, 200]
        self.vlist = ttk.Treeview(vlist_frame, columns=cols,
                                   show="headings", height=4)
        for col, w in zip(cols, widths):
            self.vlist.heading(col, text=col)
            self.vlist.column(col, width=w, minwidth=25)
        vsb = ttk.Scrollbar(vlist_frame, orient="vertical",
                             command=self.vlist.yview)
        self.vlist.configure(yscrollcommand=vsb.set)
        self.vlist.pack(side="left", fill="x", expand=True)
        vsb.pack(side="right", fill="y")

        # ── STEP 2: Quality + Save dir ────────────────────────────────────────
        s2 = tk.Frame(top, bg=BG_CARD)
        s2.pack(fill="x", pady=(0, 6))
        tk.Label(s2, text="🎚  Quality:",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(side="left")
        self.q_var = tk.StringVar(value="1080p")
        q_opts = ["best", "2160p", "1440p", "1080p", "720p",
                  "480p", "360p", "240p", "144p", "audio_mp3"]
        mk_combo(s2, self.q_var, q_opts, w=12).pack(side="left", padx=(6, 16))
        tk.Label(s2, text="💾  Save To:",
                 font=("Helvetica", 9, "bold"),
                 fg=MUTED, bg=BG_CARD).pack(side="left")
        self.dir_var = tk.StringVar(value=self.save_dir)
        Inp(s2, var=self.dir_var, w=28).pack(side="left", padx=(6, 6), ipady=4)
        Btn(s2, "📂", self._browse,
            bg=BG_INPUT, fg=MUTED, fs=9, py=4, px=8).pack(side="left")

        # ── Smart Sequential Rename ───────────────────────────────────────────
        ren_card = tk.Frame(top, bg="#111a11", padx=10, pady=6,
                            highlightbackground="#2a4a2a", highlightthickness=1)
        ren_card.pack(fill="x", pady=(0, 6))

        ren_hdr = tk.Frame(ren_card, bg="#111a11")
        ren_hdr.pack(fill="x")
        self.rename_var = tk.BooleanVar(value=True)
        tk.Checkbutton(ren_hdr, text="🔢 Smart Sequential Rename",
                       variable=self.rename_var,
                       font=("Helvetica", 9, "bold"),
                       fg=GREEN, bg="#111a11",
                       selectcolor=BG_INPUT, activebackground="#111a11",
                       activeforeground=GREEN,
                       command=self._toggle_rename_opts).pack(side="left")
        tk.Label(ren_hdr, text="  Auto-number files by playlist order",
                 font=("Helvetica", 7), fg="#5a8a5a", bg="#111a11").pack(side="left")

        self.rename_opts_frame = tk.Frame(ren_card, bg="#111a11")
        self.rename_opts_frame.pack(fill="x", pady=(4, 0))

        mode_row = tk.Frame(self.rename_opts_frame, bg="#111a11")
        mode_row.pack(fill="x")
        tk.Label(mode_row, text="Mode:",
                 font=("Helvetica", 8, "bold"), fg=MUTED, bg="#111a11").pack(side="left")
        self.rename_mode = tk.StringVar(value="resequence")
        tk.Radiobutton(mode_row, text="🔄 Re-sequence",
                       variable=self.rename_mode, value="resequence",
                       font=("Helvetica", 8), fg=WHITE, bg="#111a11",
                       selectcolor=BG_INPUT, activebackground="#111a11",
                       activeforeground=WHITE).pack(side="left", padx=(6, 0))
        tk.Radiobutton(mode_row, text="📌 Preserve Order",
                       variable=self.rename_mode, value="preserve",
                       font=("Helvetica", 8), fg=WHITE, bg="#111a11",
                       selectcolor=BG_INPUT, activebackground="#111a11",
                       activeforeground=WHITE).pack(side="left", padx=(10, 0))

        dp_row = tk.Frame(self.rename_opts_frame, bg="#111a11")
        dp_row.pack(fill="x", pady=(3, 0))
        tk.Label(dp_row, text="Dups:",
                 font=("Helvetica", 8, "bold"), fg=MUTED, bg="#111a11").pack(side="left")
        self.dup_mode = tk.StringVar(value="suffix")
        for lbl, val in [("Suffix","suffix"),("Replace","replace"),("Skip","skip")]:
            tk.Radiobutton(dp_row, text=lbl,
                           variable=self.dup_mode, value=val,
                           font=("Helvetica", 8), fg=WHITE, bg="#111a11",
                           selectcolor=BG_INPUT, activebackground="#111a11",
                           activeforeground=WHITE).pack(side="left", padx=(6, 0))
        tk.Label(dp_row, text="  Prefix:",
                 font=("Helvetica", 8, "bold"), fg=MUTED, bg="#111a11").pack(side="left", padx=(12, 0))
        self.rename_prefix = tk.StringVar()
        Inp(dp_row, var=self.rename_prefix, w=14).pack(side="left", padx=(4, 0), ipady=2)

        # ── STEP 3: Action buttons ────────────────────────────────────────────
        s3 = tk.Frame(top, bg=BG_CARD)
        s3.pack(fill="x", pady=(4, 4))
        self.start_btn = Btn(s3, "▶  Download All",
                             self._start_all, bg=GREEN, fg=WHITE, fs=10, py=7)
        self.start_btn.pack(side="left", padx=(0, 4))
        self.start_btn.config(state="disabled")
        Btn(s3, "⏸ Pause All",  self._pause_all,
            bg=ORANGE, fg=WHITE, fs=9, py=6, px=10).pack(side="left", padx=(0, 4))
        Btn(s3, "▶ Resume All", self._resume_all,
            bg=BLUE,   fg=WHITE, fs=9, py=6, px=10).pack(side="left", padx=(0, 4))
        Btn(s3, "✖ Cancel All", self._cancel_all,
            bg=DIM,    fg=WHITE, fs=9, py=6, px=10).pack(side="left", padx=(0, 4))
        Btn(s3, "🗑 Clear",     self._clear_all,
            bg=BG_INPUT, fg=MUTED, fs=9, py=6, px=8).pack(side="left", padx=(0, 4))
        Btn(s3, "📋 History",   self._show_history,
            bg=BG_INPUT, fg=MUTED, fs=9, py=6, px=8).pack(side="right")
        self.count_lbl = tk.Label(s3, text="",
                                  font=("Helvetica", 8), fg=MUTED, bg=BG_CARD)
        self.count_lbl.pack(side="right", padx=(0, 8))

        # ══════════════════════════════════════════════════════════════════════
        #  ROW 1 — Job cards (left) + Queue Log (right) — expands to fill space
        # ══════════════════════════════════════════════════════════════════════
        mid = tk.Frame(self, bg=BG)
        mid.grid(row=1, column=0, sticky="nsew", padx=8, pady=(4, 0))
        mid.columnconfigure(0, weight=1)
        mid.columnconfigure(1, weight=0)
        mid.rowconfigure(0, weight=1)

        # ── Left: scrollable job cards ────────────────────────────────────────
        jobs_frame = tk.Frame(mid, bg=BG)
        jobs_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        jobs_frame.rowconfigure(0, weight=1)
        jobs_frame.columnconfigure(0, weight=1)

        jobs_canvas = tk.Canvas(jobs_frame, bg=BG, highlightthickness=0,
                                height=120)
        jobs_vsb = ttk.Scrollbar(jobs_frame, orient="vertical",
                                 command=jobs_canvas.yview)
        jobs_canvas.configure(yscrollcommand=jobs_vsb.set)
        jobs_vsb.grid(row=0, column=1, sticky="ns")
        jobs_canvas.grid(row=0, column=0, sticky="nsew")

        self.jobs_inner = tk.Frame(jobs_canvas, bg=BG)
        ji_win = jobs_canvas.create_window((0, 0), window=self.jobs_inner,
                                           anchor="nw")
        self.jobs_inner.bind("<Configure>",
            lambda e: jobs_canvas.configure(
                scrollregion=jobs_canvas.bbox("all")))
        jobs_canvas.bind("<Configure>",
            lambda e: jobs_canvas.itemconfig(ji_win, width=e.width))
        jobs_canvas.bind("<Enter>",
            lambda e: jobs_canvas.bind_all("<MouseWheel>",
                lambda ev: jobs_canvas.yview_scroll(
                    int(-1*(ev.delta/120)), "units")))
        jobs_canvas.bind("<Leave>",
            lambda e: jobs_canvas.unbind_all("<MouseWheel>"))

        # ── Right: Queue Log ──────────────────────────────────────────────────
        log_panel = tk.Frame(mid, bg=BG_CARD, width=255)
        log_panel.grid(row=0, column=1, sticky="nsew")
        log_panel.pack_propagate(False)
        log_panel.grid_propagate(False)

        lp_hdr = tk.Frame(log_panel, bg=BG_CARD)
        lp_hdr.pack(fill="x", padx=8, pady=(8, 4))
        tk.Label(lp_hdr, text="📋 Queue Log",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(side="left")
        Btn(lp_hdr, "🗑", self._clear_log,
            bg=BG_INPUT, fg=MUTED, fs=7, py=2, px=6).pack(side="right")

        self.summary_lbl = tk.Label(log_panel, text="",
                                    font=("Helvetica", 8), fg=MUTED, bg=BG_CARD)
        self.summary_lbl.pack(anchor="w", padx=8)

        self.queue_log = scrolledtext.ScrolledText(
            log_panel, height=6,
            bg=BG_INPUT, fg=GREEN,
            insertbackground=WHITE,
            font=("Courier", 8),
            relief="flat", state="disabled", wrap="word")
        self.queue_log.pack(fill="both", expand=True, padx=6, pady=(4, 4))
        self.queue_log.tag_configure("ok",    foreground=GREEN)
        self.queue_log.tag_configure("err",   foreground=ACCENT)
        self.queue_log.tag_configure("warn",  foreground=YELLOW)
        self.queue_log.tag_configure("info",  foreground=BLUE)
        self.queue_log.tag_configure("muted", foreground=MUTED)

        Btn(log_panel, "📂 Open History",
            self._show_history,
            bg=BG_CARD, fg=MUTED, fs=8, py=4
            ).pack(fill="x", padx=6, pady=(0, 6))

        # ══════════════════════════════════════════════════════════════════════
        #  ROW 2 — Disclaimer + Scroll Nav (always visible at bottom)
        # ══════════════════════════════════════════════════════════════════════
        disc_wrap = tk.Frame(self, bg=BG)
        disc_wrap.grid(row=2, column=0, sticky="ew", padx=8, pady=(4, 6))

        disc_fr = tk.Frame(disc_wrap, bg="#0e1520", padx=10, pady=5,
                           highlightbackground="#1e3050", highlightthickness=1)
        disc_fr.pack(fill="x")

        disc_hdr = tk.Frame(disc_fr, bg="#0e1520")
        disc_hdr.pack(fill="x")
        tk.Label(disc_hdr, text="📋  Important Notice",
                 font=("Helvetica", 8, "bold"), fg="#5a9fd4",
                 bg="#0e1520").pack(side="left")

        nav_cfg = dict(font=("Helvetica", 7, "bold"), relief="flat", bd=0,
                       padx=10, pady=3, cursor="hand2")
        tk.Button(disc_hdr, text="⬆ Top", bg="#1a1a2e", fg="#5a9fd4",
                  command=lambda: jobs_canvas.yview_moveto(0),
                  **nav_cfg).pack(side="right", padx=(4, 0))
        tk.Button(disc_hdr, text="⬇ Bottom", bg="#1a1a2e", fg="#5a9fd4",
                  command=lambda: jobs_canvas.yview_moveto(1),
                  **nav_cfg).pack(side="right")

        for icon, dtxt in _DISCLAIMER_LINES:
            drow = tk.Frame(disc_fr, bg="#0e1520")
            drow.pack(fill="x", pady=1)
            tk.Label(drow, text=f"  {icon}",
                     font=("Helvetica", 8), fg="#5a9fd4", bg="#0e1520",
                     width=3).pack(side="left")
            tk.Label(drow, text=dtxt,
                     font=("Helvetica", 8), fg="#8ab4d4", bg="#0e1520",
                     anchor="w", wraplength=800, justify="left").pack(
                     side="left", fill="x", expand=True)

    # ── Queue Log ─────────────────────────────────────────────────────────────
    def _qlog(self, msg, tag="info"):
        ts = time.strftime("%H:%M:%S")
        self.queue_log.config(state="normal")
        self.queue_log.insert("end", f"[{ts}] {msg}\n", tag)
        self.queue_log.see("end")
        self.queue_log.config(state="disabled")

    def _clear_log(self):
        self.queue_log.config(state="normal")
        self.queue_log.delete("1.0", "end")
        self.queue_log.config(state="disabled")
        self.summary_lbl.config(text="")

    def _update_summary(self):
        if not self.jobs:
            return
        counts = {}
        for j in self.jobs:
            counts[j.state] = counts.get(j.state, 0) + 1
        total = len(self.jobs)
        parts = []
        for st, sym in [("done", "✅"), ("running", "⬇"), ("paused", "⏸"),
                        ("error", "❌"), ("cancelled", "✖"), ("queued", "⏳")]:
            if counts.get(st):
                parts.append(f"{sym}{counts[st]}")
        self.summary_lbl.config(
            text=f"Total {total}: " + "  ".join(parts), fg=MUTED)

    def _poll_jobs(self):
        changed = False
        for j in self.jobs:
            prev = self._last_states.get(id(j), "")
            if prev != j.state:
                changed = True
                self._last_states[id(j)] = j.state
                name = j.title[:40]
                if j.state == "running":
                    self._qlog(f"⬇ Started:   {name}", "info")
                elif j.state == "paused":
                    self._qlog(f"⏸ Paused:    {name}", "warn")
                elif j.state == "done":
                    self._qlog(f"✅ Done:      {name}", "ok")
                elif j.state == "error":
                    self._qlog(f"❌ Failed:    {name}", "err")
                elif j.state == "cancelled":
                    self._qlog(f"✖  Cancelled: {name}", "muted")
        if changed:
            self._update_summary()

        active = any(j.state in ("running", "paused", "queued")
                     for j in self.jobs)
        if active:
            self.after(600, self._poll_jobs)
        else:
            if self.jobs and not getattr(self, "_rename_done", False):
                self._rename_done = True
                done_jobs    = [j for j in self.jobs if j.state == "done"]
                failed_jobs  = [j for j in self.jobs if j.state == "error"]
                skipped_jobs = [j for j in self.jobs if j.state == "cancelled"]
                total        = len(self.jobs)
                done_count   = len(done_jobs)
                fail_count   = len(failed_jobs)
                skip_count   = len(skipped_jobs)

                rename_log = []

                # ── Smart Sequential Rename ──────────────────────────────
                if self.rename_var.get() and done_jobs:
                    self._qlog("─" * 30, "muted")
                    self._qlog("🔢 Applying Smart Sequential Rename…", "info")

                    # Build (seq_num, path, title) list — only done jobs
                    jobs_for_rename = [
                        (getattr(j, "seq_num", i+1),
                         getattr(j, "_final_path", ""),
                         j.title)
                        for i, j in enumerate(self.jobs)
                        if j.state == "done"
                    ]
                    ren_results = self._smart_rename(jobs_for_rename)
                    ren_ok  = sum(1 for r in ren_results if r[0])
                    ren_err = sum(1 for r in ren_results if not r[0])

                    for ok_r, new_path, err_msg in ren_results:
                        if not ok_r:
                            self._qlog(f"  ⚠ {err_msg[:60]}", "err")
                            rename_log.append(f"❌ {err_msg}")
                        elif err_msg == "skipped_dup":
                            self._qlog(f"  ⏭ Dup skip: {os.path.basename(new_path)[:45]}", "warn")
                        else:
                            self._qlog(f"  ✅ → {os.path.basename(new_path)[:50]}", "ok")

                    self._qlog(f"🔢 Rename done: {ren_ok} OK, {ren_err} errors", "ok" if ren_err == 0 else "warn")

                # ── Final Summary ─────────────────────────────────────────
                self._qlog("─" * 30, "muted")
                self._qlog(
                    f"Batch finished: {done_count}/{total} OK  "
                    f"| {fail_count} failed  | {skip_count} skipped",
                    "ok" if fail_count == 0 else "warn")
                self._update_summary()
                self.count_lbl.config(
                    text=f"✅ {done_count}/{total}"
                         + (f"  ❌ {fail_count}" if fail_count else ""),
                    fg=GREEN if fail_count == 0 else YELLOW)

                # ── Summary Popup ─────────────────────────────────────────
                self._show_batch_summary(
                    total, done_count, fail_count, skip_count,
                    failed_jobs, rename_log)

    def _show_batch_summary(self, total, done, failed, skipped, failed_jobs, rename_log):
        """Show a detailed summary report popup after batch completion."""
        win = tk.Toplevel(self.winfo_toplevel())
        win.title("📊 Batch Download Summary")
        win.geometry("520x460")
        win.resizable(True, True)
        win.configure(bg=BG)

        # Header
        hdr_col = GREEN if failed == 0 else (YELLOW if done > 0 else ACCENT)
        tk.Label(win, text="📊 Batch Summary Report",
                 font=("Helvetica",13,"bold"), fg=WHITE, bg=BG).pack(pady=(16,4))
        tk.Frame(win, bg=BORDER, height=1).pack(fill="x")

        # Stats grid
        stat_fr = tk.Frame(win, bg=BG_CARD, padx=20, pady=14)
        stat_fr.pack(fill="x", padx=16, pady=10)

        stats = [
            ("📦 Total Files",       str(total),  WHITE),
            ("✅ Downloaded OK",     str(done),   GREEN),
            ("❌ Failed",            str(failed), ACCENT if failed else MUTED),
            ("⏭ Skipped/Cancelled", str(skipped),YELLOW if skipped else MUTED),
        ]
        for i, (lbl, val, col) in enumerate(stats):
            tk.Label(stat_fr, text=lbl, font=("Helvetica",9),
                     fg=MUTED, bg=BG_CARD).grid(row=i, column=0, sticky="w", pady=3)
            tk.Label(stat_fr, text=val, font=("Helvetica",11,"bold"),
                     fg=col, bg=BG_CARD).grid(row=i, column=1, sticky="w", padx=(20,0), pady=3)

        if self.rename_var.get():
            tk.Label(stat_fr, text="🔢 Smart Rename", font=("Helvetica",9),
                     fg=MUTED, bg=BG_CARD).grid(row=len(stats), column=0, sticky="w", pady=3)
            tk.Label(stat_fr, text="Applied ✅", font=("Helvetica",9,"bold"),
                     fg=GREEN, bg=BG_CARD).grid(row=len(stats), column=1, sticky="w", padx=(20,0))

        # Failed list
        if failed_jobs or rename_log:
            tk.Frame(win, bg=BORDER, height=1).pack(fill="x", padx=16)
            tk.Label(win, text="Failed / Rename Errors:",
                     font=("Helvetica",9,"bold"), fg=ACCENT, bg=BG).pack(anchor="w", padx=18, pady=(8,2))
            err_log = scrolledtext.ScrolledText(
                win, height=7, bg=BG_INPUT, fg=ACCENT,
                font=("Courier",8), relief="flat", state="normal")
            err_log.pack(fill="x", padx=16, pady=(0,8))
            for j in failed_jobs:
                err_log.insert("end", f"❌ #{getattr(j,'seq_num','?')} {j.title[:55]}\n")
            for msg in rename_log:
                err_log.insert("end", f"⚠ {msg}\n")
            err_log.config(state="disabled")

        # Buttons
        tk.Frame(win, bg=BORDER, height=1).pack(fill="x")
        btm = tk.Frame(win, bg=BG, pady=10)
        btm.pack(fill="x", padx=16)

        def _open_folder():
            folder = self.save_dir
            if os.path.exists(folder):
                if os.name == "nt":
                    os.startfile(folder)
                else:
                    import subprocess as sp
                    sp.Popen(["xdg-open", folder])

        Btn(btm, "📂 Open Folder", _open_folder,
            bg=BLUE, fg=WHITE, fs=9, py=7, px=14).pack(side="left")
        tk.Button(btm, text="✖ Close",
                  font=("Helvetica",9,"bold"),
                  bg="#2a0a0a", fg=MUTED,
                  relief="flat", bd=0, padx=16, pady=7,
                  cursor="hand2",
                  command=win.destroy).pack(side="right")

    # ── FETCH ─────────────────────────────────────────────────────────────────
    def _do_fetch(self):
        raw  = self.url_box.get("1.0", "end").strip()
        urls = [u.strip() for u in raw.splitlines() if u.strip()]
        if not urls:
            messagebox.showwarning("FastTube Pro",
                                   "URL ya Playlist link daalo!"); return

        # Reset UI
        for i in self.vlist.get_children():
            self.vlist.delete(i)
        self._fetched = []
        self.start_btn.config(state="disabled")
        self.fetch_btn.config(state="disabled", text="🔍 Fetching…")
        self.fetch_lbl.config(text="Fetching…", fg=YELLOW)

        threading.Thread(target=self._fetch_thread,
                         args=(urls,), daemon=True).start()

    def _fetch_thread(self, urls):
        results = []
        for url in urls:
            pkey = detect_platform(url)
            try:
                opts = base_ydl_opts(pkey)
                opts["skip_download"] = True
                # extract_flat=True → fast playlist scan (no per-video fetch)
                opts["extract_flat"]  = "in_playlist"
                opts["quiet"]         = True

                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)

                # ── Playlist ────────────────────────────────────────────────
                if info and (info.get("_type") == "playlist"
                             or info.get("entries")):
                    entries = list(info.get("entries") or [])
                    for e in entries:
                        if not e:
                            continue
                        # Build proper watch URL
                        vid_url = (e.get("url") or
                                   e.get("webpage_url") or "")
                        if vid_url and not vid_url.startswith("http"):
                            vid_url = ("https://www.youtube.com/watch?v="
                                       + e.get("id", ""))
                        if not vid_url:
                            continue
                        title = (e.get("title") or
                                 e.get("id") or "Unknown")
                        dur   = e.get("duration", 0) or 0
                        dur_s = (f"{int(dur//60)}:{int(dur%60):02d}"
                                 if dur else "—")
                        results.append((vid_url, title, dur_s))

                # ── Single video ─────────────────────────────────────────────
                elif info:
                    title = (info.get("title") or
                             info.get("id") or "Unknown")
                    dur   = info.get("duration", 0) or 0
                    dur_s = (f"{int(dur//60)}:{int(dur%60):02d}"
                             if dur else "—")
                    results.append((url, title, dur_s))

            except Exception as e:
                err = strip_ansi(str(e))[:55]
                self.after(0, lambda m=err:
                           self.fetch_lbl.config(
                               text=f"⚠ {m}", fg=ACCENT))

        self.after(0, lambda: self._show_fetched(results))

    def _show_fetched(self, results):
        for i in self.vlist.get_children():
            self.vlist.delete(i)
        self._fetched = results
        for idx, (url, title, dur) in enumerate(results):
            short_url = (url[:55] + "…") if len(url) > 55 else url
            self.vlist.insert("", "end",
                values=(idx + 1,
                        (title[:65] + "…") if len(title) > 65 else title,
                        dur,
                        short_url))
        # Restore button
        self.fetch_btn.config(state="normal",
                              text="🔍  Fetch Videos / Playlist")
        if results:
            self.fetch_lbl.config(
                text=f"✅ {len(results)} videos — quality choose karke Download All dabao",
                fg=GREEN)
            self.start_btn.config(state="normal")
        else:
            self.fetch_lbl.config(text="❌ Koi video nahi mila", fg=ACCENT)

    # ── DOWNLOAD ──────────────────────────────────────────────────────────────
    def _toggle_rename_opts(self):
        if self.rename_var.get():
            self.rename_opts_frame.pack(fill="x", pady=(6, 0))
        else:
            self.rename_opts_frame.pack_forget()

    @staticmethod
    def _clean_title(title):
        """Remove emojis, common filler words, extra symbols for clean filenames."""
        import unicodedata
        # Remove emojis (chars outside BMP or in emoji blocks)
        cleaned = "".join(
            c for c in title
            if unicodedata.category(c) not in ("So", "Sk", "Sm")
            and ord(c) < 0x1F600 or ord(c) < 128
        )
        # Remove common filler words (case-insensitive)
        filler = [
            r'\bofficial\b', r'\bofficial video\b', r'\bofficial audio\b',
            r'\bofficial music video\b', r'\blyrics?\b', r'\blyric video\b',
            r'\bfull video\b', r'\bfull song\b', r'\bhd\b', r'\b4k\b',
            r'\b8k\b', r'\b1080p\b', r'\b720p\b', r'\bpremiere\b',
            r'\bfeat\.?\b', r'\bft\.?\b', r'\bexplicit\b', r'\bclean\b',
            r'\bslowed\b', r'\breverb\b', r'\baudio\b', r'\bvideo\b',
        ]
        for pat in filler:
            cleaned = re.sub(pat, '', cleaned, flags=re.IGNORECASE)
        # Remove brackets/parens that are now empty or have only spaces
        cleaned = re.sub(r'\(\s*\)', '', cleaned)
        cleaned = re.sub(r'\[\s*\]', '', cleaned)
        cleaned = re.sub(r'\{\s*\}', '', cleaned)
        # Collapse multiple spaces, strip edges
        cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip(" -_|")
        # Remove illegal filename chars
        cleaned = re.sub(r'[<>:"/\\|?*]', '', cleaned)
        return cleaned or title  # fallback to original if empty

    def _smart_rename(self, jobs_done):
        """
        Rename downloaded files with sequential numbers.
        jobs_done: list of (seq_num, final_path, title) in playlist order
        Returns: list of (ok, new_path, error_msg)
        """
        mode       = self.rename_mode.get()
        dup_mode   = self.dup_mode.get()
        prefix     = self.rename_prefix.get().strip()
        total      = len(jobs_done)
        pad_width  = len(str(total)) if total >= 10 else 2
        pad_width  = max(pad_width, 3) if total >= 100 else pad_width

        results = []
        seq_counter = 1  # used for re-sequence mode

        for (orig_seq, fpath, title) in jobs_done:
            if not fpath or not os.path.exists(fpath):
                results.append((False, "", f"File not found: {fpath}"))
                continue

            seq_num  = orig_seq if mode == "preserve" else seq_counter
            seq_str  = str(seq_num).zfill(pad_width)
            clean    = self._clean_title(title)
            ext      = os.path.splitext(fpath)[1]
            base     = f"{prefix}_{seq_str} - {clean}" if prefix else f"{seq_str} - {clean}"
            base     = re.sub(r'[<>:"/\\|?*]', '_', base)
            new_name = base + ext
            new_path = os.path.join(os.path.dirname(fpath), new_name)

            # ── 3-retry rename attempt ───────────────────────────────────
            renamed  = False
            last_err = ""
            for attempt in range(3):
                try:
                    if os.path.exists(new_path) and new_path != fpath:
                        if dup_mode == "skip":
                            results.append((True, new_path, "skipped_dup"))
                            renamed = True; break
                        elif dup_mode == "replace":
                            os.remove(new_path)
                        else:  # suffix
                            suf = 2
                            while os.path.exists(new_path):
                                new_path = os.path.join(
                                    os.path.dirname(fpath),
                                    f"{base}_{suf}{ext}")
                                suf += 1
                    if fpath != new_path:
                        os.rename(fpath, new_path)
                    results.append((True, new_path, ""))
                    renamed = True; seq_counter += 1
                    break
                except OSError as e:
                    last_err = str(e)
                    time.sleep(0.8)

            if not renamed:
                # Save as-is with error log
                results.append((False, fpath, f"Rename failed (3 attempts): {last_err}"))
                seq_counter += 1

        return results

    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.save_dir)
        if d:
            self.save_dir = d
            self.dir_var.set(d)

    def _start_all(self):
        if not self._fetched:
            messagebox.showwarning("FastTube Pro",
                                   "Pehle Fetch karo!"); return

        # ── Feature gate: batch ───────────────────────────────────────────────
        ok, msg, used, lim = sub_check_feature("batch")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return

        # ── Quality gate ─────────────────────────────────────────────────────
        q = self.q_var.get() if hasattr(self,'q_var') else "best"
        ok2, msg2 = sub_can_dl(q)
        if not ok2:
            show_limit_popup(self.winfo_toplevel(), msg2); return

        self.save_dir = self.dir_var.get().strip() or get_save_dir()
        os.makedirs(self.save_dir, exist_ok=True)

        # Clear old job cards + log
        for w in self.jobs_inner.winfo_children():
            w.destroy()
        self.jobs         = []
        self._last_states = {}
        self._queue_idx   = 0
        self._cancel_seq  = False
        self._rename_done = False
        self._clear_log()

        q = self.q_var.get()

        # ── Duplicate check for each URL before queuing ───────────────────────
        skipped = 0
        approved_fetched = []
        for url, title, extra in self._fetched:
            is_dup, dup_row = db_check_duplicate(url)
            if is_dup:
                go = ask_duplicate_popup(self.winfo_toplevel(), url, dup_row)
                if not go:
                    skipped += 1
                    continue
            approved_fetched.append((url, title, extra))

        if not approved_fetched:
            messagebox.showinfo("FastTube Pro",
                "Saare URLs skip kar diye gaye (duplicates).\n"
                "Koi naya download shuru nahi hua.")
            return

        for idx, (url, title, _) in enumerate(approved_fetched):
            job = BatchTab._Job(
                self.jobs_inner, idx, url, title, q, self.save_dir)
            job.seq_num = idx + 1
            self.jobs.append(job)

        total = len(self.jobs)
        self.count_lbl.config(text=f"⏳ {total} queued…", fg=YELLOW)
        skip_note = f" | {skipped} duplicate(s) skipped" if skipped else ""
        self._qlog(f"Starting {total} video(s) | Quality: {q}{skip_note}", "info")
        if self.rename_var.get():
            mode_txt = "Auto Re-sequence" if self.rename_mode.get() == "resequence" else "Preserve Order"
            self._qlog(f"🔢 Smart Rename: ON  |  Mode: {mode_txt}", "info")
        self._qlog(f"Save: {self.save_dir}", "muted")
        self._qlog("─" * 30, "muted")
        self._update_summary()

        sub_record_feature("batch")   # count once per batch session
        self._queue_idx = 0
        self._run_next_job()
        self.after(600, self._poll_jobs)

    def _run_next_job(self):
        """Sequential queue runner — starts next queued job when current finishes."""
        if self._cancel_seq:
            return
        # Find next queued job
        for idx, job in enumerate(self.jobs):
            if job.state == "queued":
                job.start()
                # Poll until this job finishes, then run next
                self.after(800, lambda j=job: self._wait_job(j))
                return
        # No more queued jobs — all done/error/cancelled

    def _wait_job(self, job):
        """Wait for job to finish, then trigger next."""
        if job.state in ("running", "paused"):
            self.after(800, lambda: self._wait_job(job))
        else:
            # Job finished (done/error/cancelled) — start next
            self.after(300, self._run_next_job)

    def _pause_all(self):
        n = 0
        for j in self.jobs:
            if j.state == "running":
                j._toggle_pause(); n += 1
        if n:
            self._qlog(f"⏸ Paused all ({n})", "warn")

    def _resume_all(self):
        n = 0
        for j in self.jobs:
            if j.state == "paused":
                j._toggle_pause(); n += 1
        if n:
            self._qlog(f"▶ Resumed all ({n})", "info")

    def _cancel_all(self):
        self._cancel_seq = True   # stop sequential queue from starting next job
        n = 0
        for j in self.jobs:
            if j.state in ("running", "paused", "queued"):
                j._do_cancel(); n += 1
        if n:
            self._qlog(f"✖ Cancelled all ({n})", "muted")
            self._update_summary()

    def _clear_all(self):
        self._cancel_seq = True   # stop sequential queue
        for w in self.jobs_inner.winfo_children():
            w.destroy()
        self.jobs         = []
        self._last_states = {}
        self._fetched     = []
        self._queue_idx   = 0
        self._cancel_seq  = False
        for i in self.vlist.get_children():
            self.vlist.delete(i)
        self.url_box.delete("1.0", "end")
        self.count_lbl.config(text="", fg=MUTED)
        self.fetch_lbl.config(text="", fg=MUTED)
        self.start_btn.config(state="disabled")
        self._clear_log()

    # ── HISTORY ───────────────────────────────────────────────────────────────
    def _show_history(self):
        win = tk.Toplevel(self)
        win.title("📋 Download History")
        win.geometry("960x480")
        win.configure(bg=BG)
        tv_style()

        hdr = tk.Frame(win, bg=BG)
        hdr.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(hdr, text="📋 Download History",
                 font=("Helvetica", 12, "bold"),
                 fg=WHITE, bg=BG).pack(side="left")
        Btn(hdr, "🗑 Clear All",
            lambda: (db_clear(), self._reload_hist(tree)),
            bg=ACCENT, fg=WHITE, fs=8, py=4, px=10).pack(side="right")
        Btn(hdr, "🔄 Refresh",
            lambda: self._reload_hist(tree),
            bg=BG_CARD, fg=MUTED, fs=8, py=4, px=10).pack(
            side="right", padx=(0, 6))

        cols   = ("Title", "Platform", "Quality", "Status", "Date", "URL")
        widths = [280, 85, 90, 70, 130, 220]
        frame  = tk.Frame(win, bg=BG)
        frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        tree = ttk.Treeview(frame, columns=cols,
                             show="headings", height=18)
        sb   = ttk.Scrollbar(frame, orient="vertical",
                              command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        for col, w in zip(cols, widths):
            tree.heading(col, text=col)
            tree.column(col, width=w, minwidth=30)
        tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        tree.tag_configure("done",      foreground=GREEN)
        tree.tag_configure("error",     foreground=ACCENT)
        tree.tag_configure("cancelled", foreground=MUTED)

        self._reload_hist(tree)

    def _reload_hist(self, tree):
        for i in tree.get_children():
            tree.delete(i)
        for row in db_all():
            title, pkey, q, st, sz, dt, url = row
            tag = st if st in ("done", "error", "cancelled") else ""
            tree.insert("", "end",
                values=(title or "—", pkey, q, st,
                        str(dt)[:16], url),
                tags=(tag,))


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 2 — SINGLE VIDEO DOWNLOADER
#  URL paste → Fetch formats → Quality choose → Download
#  Supports: YouTube, Instagram, Facebook, Twitter, MX Player, Hotstar, +more
# ══════════════════════════════════════════════════════════════════════════════
class SingleTab(tk.Frame):

    def __init__(self, parent):
        super().__init__(parent, bg=BG)
        self.save_dir = get_save_dir()
        self.info     = None
        self.fmts     = []
        self._cancel  = False
        self._pev     = threading.Event()
        self._pev.set()
        self._state   = "idle"
        self._build()

    def _build(self):
        inner = make_scrollable(self)
        c = CardFrame(inner)
        c.pack(fill="x", padx=14, pady=(12, 4))

        # Title
        tk.Label(c, text="⬇  Single Video Downloader",
                 font=("Helvetica", 13, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(c,
                 text="YouTube · Instagram · Facebook · Twitter/X · MX Player · WhatsApp · Snapchat · +1000 sites",
                 font=("Helvetica", 8),
                 fg=MUTED, bg=BG_CARD).pack(anchor="w", pady=(2, 10))

        # Platform detect label
        self.plat_lbl = tk.Label(c, text="🌐  URL paste karke Fetch dabao",
                                 font=("Helvetica", 9, "bold"),
                                 fg=MUTED, bg=BG_CARD, anchor="w")
        self.plat_lbl.pack(fill="x", pady=(0, 6))

        # URL + Fetch row
        ur = tk.Frame(c, bg=BG_CARD)
        ur.pack(fill="x", pady=(0, 8))
        self.url_var = tk.StringVar()
        self.url_var.trace("w", self._url_chg)
        Inp(ur, var=self.url_var).pack(
            side="left", fill="x", expand=True, ipady=7, ipadx=8)
        Btn(ur, "Paste", self._paste,
            bg=BG_INPUT, fg=MUTED, px=8).pack(side="left", padx=(6, 0))
        Btn(ur, "Clear", self._clear_url,
            bg=BG_INPUT, fg=MUTED, px=8).pack(side="left", padx=(4, 0))
        self.fetch_btn = Btn(ur, "🔍 Fetch", self._fetch,
                             bg=ACCENT, fg=WHITE, px=12)
        self.fetch_btn.pack(side="left", padx=(8, 0))

        # Fetched title
        self.title_lbl = tk.Label(c, text="",
                                   font=("Helvetica", 9, "italic"),
                                   fg=MUTED, bg=BG_CARD,
                                   anchor="w", wraplength=700)
        self.title_lbl.pack(fill="x", pady=(0, 6))

        # Format table
        Hdr(c, "Available Formats  (row click = select):").pack(anchor="w")
        tv_style()
        cols   = ("ID", "Ext", "Resolution", "FPS", "VCodec", "ACodec",
                  "Size", "Note")
        widths = [55, 50, 95, 45, 85, 85, 75, 180]
        tbl_f  = tk.Frame(c, bg=BG_CARD)
        tbl_f.pack(fill="x", pady=(4, 0))
        self.fmt_tv = ttk.Treeview(tbl_f, columns=cols,
                                    show="headings",
                                    height=8, selectmode="browse")
        for col, w in zip(cols, widths):
            self.fmt_tv.heading(col, text=col)
            self.fmt_tv.column(col, width=w, minwidth=25)
        sb2 = ttk.Scrollbar(tbl_f, orient="vertical",
                             command=self.fmt_tv.yview)
        self.fmt_tv.configure(yscrollcommand=sb2.set)
        self.fmt_tv.pack(side="left", fill="x", expand=True)
        sb2.pack(side="right", fill="y")
        self.fmt_tv.bind("<<TreeviewSelect>>", self._fmt_sel)

        # Quick quality radio buttons
        Hdr(c, "Quick Quality:").pack(anchor="w", pady=(8, 0))
        qr = tk.Frame(c, bg=BG_CARD)
        qr.pack(fill="x", pady=(4, 8))
        self.q_var = tk.StringVar(value="best")
        for lbl_t, val in [
            ("🏆 Best",  "best"),
            ("4K",      "2160p"),
            ("1080p",   "1080p"),
            ("720p",    "720p"),
            ("480p",    "480p"),
            ("360p",    "360p"),
            ("240p",    "240p"),
            ("🎵 Audio","audio"),
        ]:
            tk.Radiobutton(qr, text=lbl_t, variable=self.q_var, value=val,
                           font=("Helvetica", 9), bg=BG_CARD, fg=WHITE,
                           selectcolor=ACCENT, activebackground=BG_CARD,
                           cursor="hand2").pack(side="left", padx=4)

        # Save dir
        dr = tk.Frame(c, bg=BG_CARD)
        dr.pack(fill="x", pady=(0, 8))
        Hdr(dr, "Save To:").pack(side="left")
        self.dir_lbl = tk.Label(dr, text=self.save_dir,
                                 font=("Helvetica", 9),
                                 fg=MUTED, bg=BG_CARD,
                                 anchor="w", wraplength=540)
        self.dir_lbl.pack(side="left", padx=6, fill="x", expand=True)
        Btn(dr, "📂", self._browse,
            bg=BG_INPUT, fg=MUTED, fs=9, py=4, px=8).pack(side="right")

        # Download / Pause / Cancel buttons
        ctrl = tk.Frame(c, bg=BG_CARD)
        ctrl.pack(fill="x", pady=(4, 0))
        self.dl_btn = Btn(ctrl, "⬇  Download",
                          self._start_dl, bg=GREEN, fg=WHITE, fs=12, py=9)
        self.dl_btn.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.pause_btn = Btn(ctrl, "⏸  Pause",
                             self._toggle_pause, bg=ORANGE, fg=WHITE,
                             fs=11, py=9)
        self.pause_btn.pack(side="left", padx=(0, 6))
        self.pause_btn.config(state="disabled")
        self.cancel_btn = Btn(ctrl, "✖  Cancel",
                              self._do_cancel, bg=DIM, fg=MUTED, fs=11, py=9)
        self.cancel_btn.pack(side="left")
        self.cancel_btn.config(state="disabled")

        # Progress card
        pc = CardFrame(inner)
        pc.pack(fill="x", padx=14, pady=(4, 12))

        top_r = tk.Frame(pc, bg=BG_CARD)
        top_r.pack(fill="x", pady=(0, 4))
        self.dl_lbl  = tk.Label(top_r, text="Downloaded: —",
                                 font=("Helvetica", 9, "bold"),
                                 fg=WHITE, bg=BG_CARD)
        self.dl_lbl.pack(side="left")
        self.tot_lbl = tk.Label(top_r, text="",
                                 font=("Helvetica", 9), fg=MUTED, bg=BG_CARD)
        self.tot_lbl.pack(side="left", padx=(4, 0))
        self.spd_lbl = tk.Label(top_r, text="",
                                 font=("Helvetica", 9, "bold"),
                                 fg=GREEN, bg=BG_CARD)
        self.spd_lbl.pack(side="left", padx=(16, 0))
        self.eta_lbl = tk.Label(top_r, text="",
                                 font=("Helvetica", 9), fg=YELLOW, bg=BG_CARD)
        self.eta_lbl.pack(side="right")

        self.pb = mk_pb(pc)
        self.pb.pack(fill="x", pady=(0, 4))

        bot_r = tk.Frame(pc, bg=BG_CARD)
        bot_r.pack(fill="x")
        self.pct_lbl = tk.Label(bot_r, text="0%",
                                 font=("Helvetica", 14, "bold"),
                                 fg=WHITE, bg=BG_CARD)
        self.pct_lbl.pack(side="left")
        self.st_lbl  = tk.Label(bot_r, text="Ready",
                                 font=("Helvetica", 9),
                                 fg=MUTED, bg=BG_CARD, anchor="e")
        self.st_lbl.pack(side="right")

        add_disclaimer(inner)

    # ── helpers ───────────────────────────────────────────────────────────────
    def _paste(self):
        try:
            self.url_var.set(self.winfo_toplevel().clipboard_get().strip())
        except Exception:
            pass

    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.save_dir)
        if d:
            self.save_dir = d
            self.dir_lbl.config(text=d)

    def _clear_url(self):
        self.url_var.set("")
        self.title_lbl.config(text="")
        for i in self.fmt_tv.get_children():
            self.fmt_tv.delete(i)
        self.fmts = []
        self.info = None
        self.pb.config(value=0)
        self.pct_lbl.config(text="0%")
        self.st_lbl.config(text="Ready", fg=MUTED)

    def _url_chg(self, *_):
        url = self.url_var.get().strip()
        if not url:
            self.plat_lbl.config(
                text="🌐  URL paste karke Fetch dabao", fg=MUTED)
            return
        p = detect_platform(url)
        if p in PLATFORMS:
            inf = PLATFORMS[p]
            self.plat_lbl.config(
                text=f"{inf['icon']}  {inf['name']} detected ✓",
                fg=inf["color"])
        else:
            self.plat_lbl.config(text="🌐  Platform detected", fg=MUTED)

    def _set_st(self, msg, c=MUTED):
        self.st_lbl.config(text=msg, fg=c)
        self.winfo_toplevel().update_idletasks()

    # ── Fetch formats ─────────────────────────────────────────────────────────
    def _fetch(self):
        url = self.url_var.get().strip()
        if not url:
            messagebox.showwarning("FastTube Pro", "URL daalo!"); return
        self._set_st("Fetching…", YELLOW)
        self.fetch_btn.config(state="disabled")
        threading.Thread(target=self._fetch_th, args=(url,),
                         daemon=True).start()

    def _fetch_th(self, url):
        pkey = detect_platform(url)
        info = None
        last_err = ""
        try:
            # MX Player: use dedicated fetch function
            if pkey == "mxplayer":
                info = mx_fetch_info(url)
            else:
                max_ci = len(PLAYER_CLIENTS) if pkey == "youtube" else 2
                for ci in range(max_ci):
                    try:
                        o = base_ydl_opts(pkey, ci)
                        o["skip_download"] = True
                        with yt_dlp.YoutubeDL(o) as ydl:
                            info = ydl.extract_info(url, download=False)
                        if info:
                            break
                    except Exception as e:
                        last_err = strip_ansi(str(e))
                        time.sleep(1)
        except Exception as e:
            last_err = strip_ansi(str(e))

        self.after(0, lambda: self.fetch_btn.config(state="normal"))
        if not info:
            short = last_err[:120] if last_err else "Unknown error"
            self.after(0, lambda m=short: self._set_st(f"❌ {m}", ACCENT))
            return
        self.info = info
        self.after(0, lambda: self._populate(info))

    def _populate(self, info):
        for i in self.fmt_tv.get_children():
            self.fmt_tv.delete(i)
        self.fmts = []
        title = info.get("title", "")
        self.title_lbl.config(text=f"🎬 {title[:85]}", fg=WHITE)

        # Pinned convenience rows
        self.fmt_tv.insert("", "end", iid="_best",
            values=("best", "mp4", "🏆 Best",
                    "—", "—", "—", "—", "Auto best video+audio"))
        self.fmt_tv.insert("", "end", iid="_audio",
            values=("audio", "mp3", "🎵 Audio",
                    "—", "—", "—", "—", "Best audio → MP3 320k"))

        # All real formats
        fmts = info.get("formats", []) or []
        seen = set()
        for f in reversed(fmts):
            fid = f.get("format_id", "")
            ext = f.get("ext", "")
            h   = f.get("height")
            w   = f.get("width")
            fps = f.get("fps", "—") or "—"
            fs  = f.get("filesize") or f.get("filesize_approx", 0)
            vco = (f.get("vcodec", "none") or "none")[:12]
            aco = (f.get("acodec", "none") or "none")[:12]
            res = (f"{w}x{h}" if w and h
                   else (f"{h}p" if h else "audio"))
            sz  = f"{fs/1024/1024:.1f}MB" if fs else "—"
            key = (res, ext)
            if key in seen:
                continue
            seen.add(key)
            self.fmt_tv.insert("", "end",
                values=(fid, ext, res, fps, vco, aco, sz,
                        f.get("format_note", "")))
            self.fmts.append(f)

        self._set_st(
            f"✅ {len(fmts)} formats — row click ya Quick Quality choose karo",
            GREEN)

    def _fmt_sel(self, _):
        sel = self.fmt_tv.selection()
        if sel:
            v   = self.fmt_tv.item(sel[0])["values"]
            fid = str(v[0]) if v else ""
            if "audio" in fid:
                self.q_var.set("audio")
            elif fid in ("best", "_best"):
                self.q_var.set("best")

    # ── Download ──────────────────────────────────────────────────────────────
    def _start_dl(self):
        url = self.url_var.get().strip()
        if not url:
            messagebox.showwarning("FastTube Pro", "URL daalo!"); return

        # ── Duplicate check ──────────────────────────────────────────────────
        is_dup, dup_row = db_check_duplicate(url)
        if is_dup:
            if not ask_duplicate_popup(self.winfo_toplevel(), url, dup_row):
                return

        # ── Feature gate: single ─────────────────────────────────────────────
        ok_f, msg_f, _, _ = sub_check_feature("single")
        if not ok_f:
            show_limit_popup(self.winfo_toplevel(), msg_f); return

        # ── Quality gate ─────────────────────────────────────────────────────
        q_check = self.q_var.get() if hasattr(self,'q_var') else "best"
        ok, msg = sub_can_dl(q_check)
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return

        sel = self.fmt_tv.selection()
        fid = ""
        if sel:
            v   = self.fmt_tv.item(sel[0])["values"]
            fid = str(v[0]) if v else ""

        q = self.q_var.get()
        # Determine yt-dlp format string
        if fid and fid not in ("best", "_best", "audio", "_audio", ""):
            fmt = (f"{fid}+bestaudio"
                   f"/bestvideo[format_id={fid}]+bestaudio/{fid}")
        elif fid in ("audio", "_audio") or q == "audio":
            fmt = "bestaudio"
        elif fid in ("best", "_best") or q == "best":
            fmt = "bestvideo+bestaudio/best"
        elif q.endswith("p") and q[:-1].isdigit():
            h   = int(q[:-1])
            fmt = (f"bestvideo[height<={h}][ext=mp4]"
                   f"+bestaudio[ext=m4a]"
                   f"/bestvideo[height<={h}]+bestaudio"
                   f"/best[height<={h}]/best")
        else:
            fmt = "bestvideo+bestaudio/best"

        is_audio = (fid in ("audio", "_audio") or q == "audio")
        pp = ([{"key": "FFmpegExtractAudio",
                "preferredcodec": "mp3", "preferredquality": "320"}]
              if is_audio else [])

        self._cancel = False
        self._pev.set()
        self._state = "running"
        self.pb.config(value=0)
        self.pct_lbl.config(text="0%", fg=WHITE)
        self.dl_btn.config(state="disabled", text="⬇ Downloading…")
        self.pause_btn.config(state="normal")
        self.cancel_btn.config(state="normal", bg=DIM, fg=WHITE)

        threading.Thread(target=self._dl_th,
                         args=(url, fmt, pp, q),
                         daemon=True).start()

    def _dl_th(self, url, fmt, pp, q_lbl):
        pkey  = detect_platform(url)
        title = sanitize(
            (self.info.get("title", "") if self.info else "") or "media")
        out   = os.path.join(self.save_dir, f"{title}.%(ext)s")

        def hook(d):
            if self._cancel:
                raise Exception("__CANCELLED__")
            if not self._pev.is_set():
                self._pev.wait()
                if self._cancel:
                    raise Exception("__CANCELLED__")
            if d["status"] == "downloading":
                tb  = (d.get("total_bytes")
                       or d.get("total_bytes_estimate") or 0)
                db  = d.get("downloaded_bytes", 0)
                pct = (db / tb * 100) if tb else 0
                self.after(0, lambda p=pct,
                    dm=to_mb(db), tm=to_mb(tb),
                    sp=d.get("_speed_str", "").strip(),
                    et=d.get("_eta_str", "").strip(): (
                    self.pb.config(value=p),
                    self.pct_lbl.config(text=f"{p:.0f}%"),
                    self.dl_lbl.config(text=f"Downloaded: {dm:.1f}MB"),
                    self.tot_lbl.config(text=f"/ {tm:.1f}MB") if tm > 0
                        else None,
                    self.spd_lbl.config(text=f"⚡{sp}") if sp else None,
                    self.eta_lbl.config(text=f"ETA:{et}") if et else None,
                ))
            elif d["status"] == "finished":
                self.after(0,
                    lambda: self._set_st("⚙ Merging…", YELLOW))

        max_ci = len(PLAYER_CLIENTS) if pkey == "youtube" else 2
        # MX Player only needs 1 attempt with special opts
        if pkey == "mxplayer":
            max_ci = 1
        for ci in range(max_ci):
            if self._cancel:
                self.after(0, self._on_cancel)
                return
            try:
                o = base_ydl_opts(pkey, ci)
                o.update({
                    "format":              fmt,
                    "outtmpl":             out,
                    "progress_hooks":      [hook],
                    "merge_output_format": "mp4",
                    "postprocessors":      pp,
                    "noplaylist":          True,
                })
                # MX Player: allow HLS/DASH, don't force mp4 merge
                if pkey == "mxplayer":
                    o["merge_output_format"] = "mkv"
                    o["hls_prefer_native"]   = False
                with yt_dlp.YoutubeDL(o) as ydl:
                    ydl.download([url])
                if self._cancel:
                    self.after(0, self._on_cancel)
                else:
                    dur = ""
                    if self.info:
                        d = self.info.get("duration", 0)
                        if d:
                            dur = f"{int(d//60)}:{int(d%60):02d}"
                    db_insert(url, title, pkey, q_lbl, "", "done", 0, dur)
                    sub_record_dl()
                    sub_record_feature("single")   # per-feature counter
                    self.after(0, lambda: self._on_done(title))
                return
            except Exception as e:
                err = strip_ansi(str(e))
                if "__CANCELLED__" in err or self._cancel:
                    self.after(0, self._on_cancel)
                    return
                if any(k in err.lower() for k in
                       ["player", "bot", "jsinterp", "403",
                        "sign in", "login"]):
                    continue
                self.after(0, lambda m=err[:120]: self._on_err(m))
                return
        self.after(0,
            lambda: self._on_err("All clients failed. Try cookies."))

    def _toggle_pause(self):
        if self._state == "running":
            self._pev.clear()
            self._state = "paused"
            self.pause_btn.config(text="▶ Resume", bg=GREEN)
            self._set_st("⏸ Paused", YELLOW)
        elif self._state == "paused":
            self._pev.set()
            self._state = "running"
            self.pause_btn.config(text="⏸ Pause", bg=ORANGE)
            self._set_st("▶ Resuming…", GREEN)

    def _do_cancel(self):
        self._cancel = True
        self._pev.set()

    def _on_done(self, title):
        self._state = "done"
        self.pb.config(value=100)
        self.pct_lbl.config(text="100%", fg=GREEN)
        self._set_st("✅ Done!", GREEN)
        self.dl_btn.config(state="normal", text="⬇  Download")
        self.pause_btn.config(state="disabled")
        self.cancel_btn.config(state="disabled", bg=DIM, fg=MUTED)
        messagebox.showinfo("FastTube Pro",
                            f"✅ Done!\nSaved to:\n{self.save_dir}")

    def _on_cancel(self):
        self._state = "idle"
        self.pb.config(value=0)
        self._set_st("✖ Cancelled", MUTED)
        self.dl_btn.config(state="normal", text="⬇  Download")
        self.pause_btn.config(state="disabled")
        self.cancel_btn.config(state="disabled", bg=DIM, fg=MUTED)

    def _on_err(self, msg):
        self._state = "error"
        self._set_st(f"❌ {msg}", ACCENT)
        self.dl_btn.config(state="normal", text="⬇  Download")
        self.pause_btn.config(state="disabled")
        self.cancel_btn.config(state="disabled", bg=DIM, fg=MUTED)
        messagebox.showerror("Error", msg)



class AudioTab(tk.Frame):
    FMTS = [
        ("MP3 — 320 kbps ★","mp3","320"),
        ("MP3 — 256 kbps","mp3","256"),
        ("MP3 — 192 kbps","mp3","192"),
        ("MP3 — 128 kbps","mp3","128"),
        ("WAV — Lossless","wav","0"),
        ("FLAC — Lossless","flac","0"),
        ("M4A — 256 kbps","m4a","256"),
        ("OPUS — 160 kbps","opus","160"),
        ("AAC — 256 kbps","aac","256"),
    ]
    def __init__(self,parent):
        super().__init__(parent,bg=BG)
        self.save_dir=get_save_dir(); self._cancel=False
        self._pev=threading.Event(); self._pev.set(); self._state="idle"
        self._build()
    def _build(self):
        inner=make_scrollable(self); c=CardFrame(inner); c.pack(fill="x",padx=14,pady=(12,4))
        tk.Label(c,text="🎵 Audio Extractor — MP3 · WAV · FLAC · M4A",font=("Helvetica",13,"bold"),fg=WHITE,bg=BG_CARD).pack(anchor="w")
        tk.Label(c,text="Video URL se audio extract karo — metadata + thumbnail embed",font=("Helvetica",8),fg=MUTED,bg=BG_CARD).pack(anchor="w",pady=(2,10))
        Hdr(c,"URL:").pack(anchor="w")
        ur=tk.Frame(c,bg=BG_CARD); ur.pack(fill="x",pady=(4,8))
        self.url_var=tk.StringVar()
        Inp(ur,var=self.url_var).pack(side="left",fill="x",expand=True,ipady=7,ipadx=8)
        Btn(ur,"Paste",lambda: self.url_var.set(self.winfo_toplevel().clipboard_get().strip()),bg=BG_INPUT,fg=MUTED,px=8).pack(side="left",padx=6)
        Hdr(c,"Multiple URLs (one per line):").pack(anchor="w")
        self.multi=scrolledtext.ScrolledText(c,height=4,bg=BG_INPUT,fg=WHITE,insertbackground=WHITE,
            font=("Helvetica",10),relief="flat",highlightthickness=1,highlightbackground=BORDER,highlightcolor=ACCENT)
        self.multi.pack(fill="x",pady=(4,10))
        r2=tk.Frame(c,bg=BG_CARD); r2.pack(fill="x",pady=(0,10))
        Hdr(r2,"Format:").pack(side="left")
        self.fmt_var=tk.StringVar(value="MP3 — 320 kbps ★")
        mk_combo(r2,self.fmt_var,[f[0] for f in self.FMTS],w=22).pack(side="left",padx=(6,0))
        op=tk.Frame(c,bg=BG_CARD); op.pack(fill="x",pady=(0,10))
        self.meta_var=tk.BooleanVar(value=True); self.thumb_var=tk.BooleanVar(value=True)
        for txt,var in [("Retain Metadata",self.meta_var),("Embed Thumbnail (MP3)",self.thumb_var)]:
            tk.Checkbutton(op,text=txt,variable=var,bg=BG_CARD,fg=WHITE,selectcolor=ACCENT,
                activebackground=BG_CARD,font=("Helvetica",9)).pack(side="left",padx=(0,18))
        dr=tk.Frame(c,bg=BG_CARD); dr.pack(fill="x",pady=(0,8))
        Hdr(dr,"Save To:").pack(side="left")
        self.dir_lbl=tk.Label(dr,text=self.save_dir,font=("Helvetica",9),fg=MUTED,bg=BG_CARD,anchor="w",wraplength=510)
        self.dir_lbl.pack(side="left",padx=6,fill="x",expand=True)
        Btn(dr,"📂",self._browse,bg=BG_INPUT,fg=MUTED,fs=9,py=4,px=8).pack(side="right")
        ctrl=tk.Frame(c,bg=BG_CARD); ctrl.pack(fill="x",pady=(4,0))
        self.dl_btn=Btn(ctrl,"🎵  Extract Audio",self._start,bg=ACCENT,fg=WHITE,fs=12,py=9)
        self.dl_btn.pack(side="left",fill="x",expand=True,padx=(0,6))
        self.pause_btn=Btn(ctrl,"⏸  Pause",self._toggle_pause,bg=ORANGE,fg=WHITE,fs=11,py=9)
        self.pause_btn.pack(side="left",padx=(0,6)); self.pause_btn.config(state="disabled")
        self.cancel_btn=Btn(ctrl,"✖  Cancel",self._cancel_dl,bg=DIM,fg=MUTED,fs=11,py=9)
        self.cancel_btn.pack(side="left"); self.cancel_btn.config(state="disabled")
        pc=CardFrame(inner); pc.pack(fill="x",padx=14,pady=(4,12))
        self.pb=mk_pb(pc); self.pb.pack(fill="x",pady=(0,4))
        br=tk.Frame(pc,bg=BG_CARD); br.pack(fill="x")
        self.pct_lbl=tk.Label(br,text="0%",font=("Helvetica",14,"bold"),fg=WHITE,bg=BG_CARD); self.pct_lbl.pack(side="left")
        self.st_lbl=tk.Label(br,text="Ready",font=("Helvetica",9),fg=MUTED,bg=BG_CARD,anchor="e"); self.st_lbl.pack(side="right")
        self.spd_lbl=tk.Label(pc,text="",font=("Helvetica",8),fg=GREEN,bg=BG_CARD); self.spd_lbl.pack(anchor="w")
        add_disclaimer(inner)
    def _browse(self):
        d=filedialog.askdirectory(initialdir=self.save_dir)
        if d: self.save_dir=d; self.dir_lbl.config(text=d)
    def _set_st(self,msg,c=MUTED):
        self.st_lbl.config(text=msg,fg=c); self.winfo_toplevel().update_idletasks()
    def _start(self):
        ok, msg, _, _ = sub_check_feature("audio")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return
        urls=[]; u=self.url_var.get().strip()
        if u: urls.append(u)
        urls+=[x.strip() for x in self.multi.get("1.0","end").strip().splitlines() if x.strip()]
        if not urls: messagebox.showwarning("FastTube Pro","URL daalo!"); return

        # ── Duplicate check ──────────────────────────────────────────────────
        approved = []
        for url in urls:
            is_dup, dup_row = db_check_duplicate(url)
            if is_dup:
                if not ask_duplicate_popup(self.winfo_toplevel(), url, dup_row):
                    continue
            approved.append(url)
        if not approved:
            messagebox.showinfo("FastTube Pro",
                "Saare URLs skip (duplicates). Koi download shuru nahi hua."); return
        urls = approved

        fl=self.fmt_var.get(); fi=next((f for f in self.FMTS if f[0]==fl),self.FMTS[0])
        codec,quality=fi[1],fi[2]
        self._cancel=False; self._pev.set(); self._state="running"
        self.pb.config(value=0); self.pct_lbl.config(text="0%",fg=WHITE)
        self.dl_btn.config(state="disabled",text="🎵 Extracting…")
        self.pause_btn.config(state="normal"); self.cancel_btn.config(state="normal",bg=DIM,fg=WHITE)
        threading.Thread(target=self._run,args=(urls,codec,quality),daemon=True).start()
    def _run(self,urls,codec,quality):
        total=len(urls)
        for i,url in enumerate(urls):
            if self._cancel: break
            self.after(0,lambda i=i,t=total: self._set_st(f"Processing {i+1}/{t}…",YELLOW))
            pkey=detect_platform(url)
            def hook(d):
                if self._cancel: raise Exception("__CANCELLED__")
                if not self._pev.is_set():
                    self._pev.wait()
                    if self._cancel: raise Exception("__CANCELLED__")
                if d["status"]=="downloading":
                    tb=d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    db=d.get("downloaded_bytes",0); pct=(db/tb*100) if tb else 0
                    self.after(0,lambda p=pct,s=d.get("_speed_str","").strip():
                        (self.pb.config(value=p),self.pct_lbl.config(text=f"{p:.0f}%"),
                         self.spd_lbl.config(text=f"⚡{s}")))
                elif d["status"]=="finished":
                    self.after(0,lambda: self._set_st("⚙ Converting…",YELLOW))
            try:
                io=base_ydl_opts(pkey); io["skip_download"]=True
                with yt_dlp.YoutubeDL(io) as ydl: info=ydl.extract_info(url,download=False)
                title=sanitize(info.get("title","") or f"audio_{i+1}")
            except Exception: title=f"audio_{i+1}"
            pp=[{"key":"FFmpegExtractAudio","preferredcodec":codec,"preferredquality":quality if quality!="0" else "0"}]
            if self.meta_var.get(): pp.append({"key":"FFmpegMetadataPP"})
            if self.thumb_var.get() and codec=="mp3": pp.append({"key":"EmbedThumbnail"})
            out=os.path.join(self.save_dir,f"{title}.%(ext)s")
            opts=base_ydl_opts(pkey)
            opts.update({"format":"bestaudio","outtmpl":out,"progress_hooks":[hook],
                         "postprocessors":pp,"noplaylist":True,
                         "writethumbnail":self.thumb_var.get() and codec=="mp3"})
            try:
                with yt_dlp.YoutubeDL(opts) as ydl: ydl.download([url])
                db_insert(url,title,pkey,f"{codec.upper()} {quality}k","","done")
                sub_record_feature("audio")
            except Exception as e:
                err=strip_ansi(str(e))
                if "__CANCELLED__" in err or self._cancel: break
                self.after(0,lambda m=err[:120]: messagebox.showerror("Error",f"Failed:\n{m}"))
        if not self._cancel: self.after(0,self._on_done)
        else: self.after(0,self._on_cancel)
    def _on_done(self):
        self._state="done"; self.pb.config(value=100); self.pct_lbl.config(text="100%",fg=GREEN)
        self._set_st("✅ Done!",GREEN); self.dl_btn.config(state="normal",text="🎵  Extract Audio")
        self.pause_btn.config(state="disabled"); self.cancel_btn.config(state="disabled",bg=DIM,fg=MUTED)
        messagebox.showinfo("FastTube Pro",f"✅ Audio extracted!\nSaved to:\n{self.save_dir}")
    def _on_cancel(self):
        self._state="idle"; self.pb.config(value=0); self._set_st("✖ Cancelled",MUTED)
        self.dl_btn.config(state="normal",text="🎵  Extract Audio")
        self.pause_btn.config(state="disabled"); self.cancel_btn.config(state="disabled",bg=DIM,fg=MUTED)
    def _toggle_pause(self):
        if self._state=="running":
            self._pev.clear(); self._state="paused"; self.pause_btn.config(text="▶ Resume",bg=GREEN); self._set_st("⏸ Paused",YELLOW)
        elif self._state=="paused":
            self._pev.set(); self._state="running"; self.pause_btn.config(text="⏸ Pause",bg=ORANGE); self._set_st("▶ Resuming…",GREEN)
    def _cancel_dl(self): self._cancel=True; self._pev.set()


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 4 — FORMAT CONVERTER
# ══════════════════════════════════════════════════════════════════════════════
class ConverterTab(tk.Frame):
    PRESETS={
        "High Quality":    {"vb":"8000k","ab":"320k","preset":"slow"},
        "Balanced":        {"vb":"4000k","ab":"192k","preset":"medium"},
        "Compressed":      {"vb":"1500k","ab":"128k","preset":"fast"},
        "Ultra Compressed":{"vb":"800k", "ab":"96k", "preset":"ultrafast"},
        "Custom":          None,
    }
    OUT_FMTS=["mp4","mkv","mov","webm","avi","flv"]
    def __init__(self,parent):
        super().__init__(parent,bg=BG); self.save_dir=get_save_dir()
        self._proc=None; self._cancel=False; self._build()
    def _build(self):
        inner=make_scrollable(self); c=CardFrame(inner); c.pack(fill="x",padx=14,pady=(12,4))
        tk.Label(c,text="🔄 Format Converter — MP4 · MKV · MOV · WebM",font=("Helvetica",13,"bold"),fg=WHITE,bg=BG_CARD).pack(anchor="w")
        tk.Label(c,text="Local file ya URL se download + convert",font=("Helvetica",8),fg=MUTED,bg=BG_CARD).pack(anchor="w",pady=(2,10))
        if not ffmpeg_ok():
            tk.Label(c,text="⚠  FFmpeg not found — winget install ffmpeg",font=("Helvetica",10,"bold"),fg=YELLOW,bg=BG_CARD).pack(anchor="w",pady=8)
        Hdr(c,"Input File (local):").pack(anchor="w")
        ir=tk.Frame(c,bg=BG_CARD); ir.pack(fill="x",pady=(4,8))
        self.in_var=tk.StringVar()
        Inp(ir,var=self.in_var).pack(side="left",fill="x",expand=True,ipady=7,ipadx=8)
        Btn(ir,"Browse",self._browse_in,bg=BG_INPUT,fg=MUTED,px=10).pack(side="left",padx=6)
        Hdr(c,"OR URL (Download + Convert):").pack(anchor="w")
        ur=tk.Frame(c,bg=BG_CARD); ur.pack(fill="x",pady=(4,10))
        self.url_var=tk.StringVar()
        Inp(ur,var=self.url_var).pack(side="left",fill="x",expand=True,ipady=7,ipadx=8)
        Btn(ur,"Paste",lambda: self.url_var.set(self.winfo_toplevel().clipboard_get().strip()),bg=BG_INPUT,fg=MUTED,px=8).pack(side="left",padx=6)
        r2=tk.Frame(c,bg=BG_CARD); r2.pack(fill="x",pady=(0,10))
        Hdr(r2,"Output:").pack(side="left")
        self.out_var=tk.StringVar(value="mp4")
        mk_combo(r2,self.out_var,self.OUT_FMTS,w=8).pack(side="left",padx=(6,20))
        Hdr(r2,"Preset:").pack(side="left")
        self.preset_var=tk.StringVar(value="Balanced")
        mk_combo(r2,self.preset_var,list(self.PRESETS.keys()),w=18).pack(side="left",padx=6)
        self.preset_var.trace("w",self._preset_chg)
        r3=tk.Frame(c,bg=BG_CARD); r3.pack(fill="x",pady=(0,10))
        Hdr(r3,"Video Bitrate:").pack(side="left")
        self.vb_var=tk.StringVar(value="4000k")
        Inp(r3,var=self.vb_var,w=9).pack(side="left",padx=(6,20),ipady=4)
        Hdr(r3,"Audio Bitrate:").pack(side="left")
        self.ab_var=tk.StringVar(value="192k")
        Inp(r3,var=self.ab_var,w=9).pack(side="left",padx=6,ipady=4)
        dr=tk.Frame(c,bg=BG_CARD); dr.pack(fill="x",pady=(0,8))
        Hdr(dr,"Save To:").pack(side="left")
        self.dir_lbl=tk.Label(dr,text=self.save_dir,font=("Helvetica",9),fg=MUTED,bg=BG_CARD,anchor="w",wraplength=490)
        self.dir_lbl.pack(side="left",padx=6,fill="x",expand=True)
        Btn(dr,"📂",self._browse_out,bg=BG_INPUT,fg=MUTED,fs=9,py=4,px=8).pack(side="right")
        ctrl=tk.Frame(c,bg=BG_CARD); ctrl.pack(fill="x",pady=(4,0))
        self.cv_btn=Btn(ctrl,"🔄  Convert",self._start,bg=BLUE,fg=WHITE,fs=12,py=9)
        self.cv_btn.pack(side="left",fill="x",expand=True,padx=(0,6))
        self.cx_btn=Btn(ctrl,"✖  Cancel",self._do_cancel,bg=DIM,fg=MUTED,fs=11,py=9)
        self.cx_btn.pack(side="left"); self.cx_btn.config(state="disabled")
        pc=CardFrame(inner); pc.pack(fill="x",padx=14,pady=(4,12))
        self.log=scrolledtext.ScrolledText(pc,height=8,bg=BG_INPUT,fg=GREEN,font=("Courier",8),relief="flat",state="disabled")
        self.log.pack(fill="x",pady=(0,6))
        self.pb=mk_pb(pc); self.pb.pack(fill="x",pady=(0,4))
        self.st_lbl=tk.Label(pc,text="Ready",font=("Helvetica",9),fg=MUTED,bg=BG_CARD,anchor="e")
        self.st_lbl.pack(anchor="e")
        add_disclaimer(inner)
    def _browse_in(self):
        f=filedialog.askopenfilename(title="Select video",
            filetypes=[("Video","*.mp4 *.mkv *.mov *.webm *.avi *.flv *.wmv"),("All","*.*")])
        if f: self.in_var.set(f)
    def _browse_out(self):
        d=filedialog.askdirectory(initialdir=self.save_dir)
        if d: self.save_dir=d; self.dir_lbl.config(text=d)
    def _preset_chg(self,*_):
        p=self.preset_var.get()
        if p!="Custom" and self.PRESETS.get(p):
            self.vb_var.set(self.PRESETS[p]["vb"]); self.ab_var.set(self.PRESETS[p]["ab"])
    def _log(self,t):
        self.log.config(state="normal"); self.log.insert("end",t+"\n"); self.log.see("end"); self.log.config(state="disabled")
    def _set_st(self,msg,c=MUTED):
        self.st_lbl.config(text=msg,fg=c); self.winfo_toplevel().update_idletasks()
    def _start(self):
        ok, msg, _, _ = sub_check_feature("convert")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return
        if not ffmpeg_ok(): messagebox.showerror("Error","FFmpeg not found!\nwinget install ffmpeg"); return
        url=self.url_var.get().strip(); infile=self.in_var.get().strip()
        if not url and not infile: messagebox.showwarning("FastTube Pro","File ya URL daalo!"); return

        # ── Duplicate check (URL mode only) ──────────────────────────────────
        if url:
            is_dup, dup_row = db_check_duplicate(url)
            if is_dup and not ask_duplicate_popup(self.winfo_toplevel(), url, dup_row):
                return

        self._cancel=False; self.cv_btn.config(state="disabled",text="🔄 Converting…")
        self.cx_btn.config(state="normal",bg=DIM,fg=WHITE)
        self.log.config(state="normal"); self.log.delete("1.0","end"); self.log.config(state="disabled")
        self.pb.config(value=0)
        threading.Thread(target=self._run,args=(url,infile),daemon=True).start()
    def _run(self,url,infile):
        out_fmt=self.out_var.get(); vb=self.vb_var.get(); ab=self.ab_var.get()
        pk=self.preset_var.get(); ff_pre=(self.PRESETS.get(pk) or {}).get("preset","medium")
        if url:
            self.after(0,lambda: (self._set_st("⬇ Downloading…",YELLOW),self._log("Downloading…")))
            pkey=detect_platform(url)
            try:
                io=base_ydl_opts(pkey); io["skip_download"]=True
                with yt_dlp.YoutubeDL(io) as ydl: inf=ydl.extract_info(url,download=False)
                title=sanitize(inf.get("title","") or "video")
            except Exception: title="video"
            tmp=os.path.join(self.save_dir,f"_tmp_{title}.%(ext)s")
            dlop=base_ydl_opts(pkey); dlop.update({"format":"bestvideo+bestaudio/best","outtmpl":tmp,"noplaylist":True})
            try:
                with yt_dlp.YoutubeDL(dlop) as ydl: ydl.download([url])
                for f in os.listdir(self.save_dir):
                    if f.startswith(f"_tmp_{title}"): infile=os.path.join(self.save_dir,f); break
                self.after(0,lambda p=infile: self._log(f"Downloaded: {p}"))
            except Exception as e:
                self.after(0,lambda m=str(e): (self._log(f"DL Error: {m}"),self._set_st("❌ Failed",ACCENT)))
                self.after(0,self._cv_done); return
        if not infile or not os.path.exists(infile):
            self.after(0,lambda: (self._set_st("❌ File not found",ACCENT),self._log("Input not found!"))); self.after(0,self._cv_done); return
        name=os.path.splitext(os.path.basename(infile))[0]
        outfile=os.path.join(self.save_dir,f"{name}_converted.{out_fmt}")
        self.after(0,lambda: (self._set_st("🔄 Converting…",BLUE),self._log(f"Input: {infile}"),self._log(f"Output: {outfile}")))
        ff_exe=FFMPEG_PATH if FFMPEG_PATH and FFMPEG_PATH!="ffmpeg" else "ffmpeg"
        if out_fmt=="webm":
            cmd=[ff_exe,"-i",infile,"-c:v","libvpx-vp9","-b:v",vb,"-c:a","libvorbis","-b:a",ab,"-y",outfile]
        elif out_fmt=="mkv":
            cmd=[ff_exe,"-i",infile,"-c:v","copy","-c:a","copy","-y",outfile]
        else:
            cmd=[ff_exe,"-i",infile,"-c:v","libx264","-b:v",vb,"-preset",ff_pre,"-c:a","aac","-b:a",ab,"-movflags","+faststart","-y",outfile]
        try:
            self._proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)
            dur_re=re.compile(r"Duration: (\d+):(\d+):(\d+)"); time_re=re.compile(r"time=(\d+):(\d+):(\d+)"); total_s=0
            for line in self._proc.stdout:
                if self._cancel: self._proc.terminate(); break
                m=dur_re.search(line)
                if m: total_s=int(m.group(1))*3600+int(m.group(2))*60+int(m.group(3))
                m2=time_re.search(line)
                if m2 and total_s:
                    cur=int(m2.group(1))*3600+int(m2.group(2))*60+int(m2.group(3))
                    self.after(0,lambda p=min(100,cur/total_s*100): self.pb.config(value=p))
                if any(k in line.lower() for k in ("error","invalid","failed")):
                    self.after(0,lambda l=line.strip(): self._log(l))
            self._proc.wait()
            if self._proc.returncode==0 and not self._cancel:
                self.after(0,lambda: (self._set_st("✅ Converted!",GREEN),self._log(f"✅ Saved: {outfile}"),self.pb.config(value=100),
                    messagebox.showinfo("FastTube Pro",f"✅ Converted!\n{outfile}")))
                if url and infile and "_tmp_" in str(infile):
                    try: os.remove(infile)
                    except Exception: pass
            elif not self._cancel:
                self.after(0,lambda: (self._set_st("❌ Failed",ACCENT),self._log("Failed!")))
        except Exception as e:
            self.after(0,lambda m=str(e): (self._set_st("❌ Error",ACCENT),self._log(f"Error: {m}")))
        self.after(0,self._cv_done)
    def _cv_done(self):
        self.cv_btn.config(state="normal",text="🔄  Convert"); self.cx_btn.config(state="disabled",bg=DIM,fg=MUTED)
    def _do_cancel(self):
        self._cancel=True
        if self._proc:
            try: self._proc.terminate()
            except Exception: pass
        self._set_st("✖ Cancelled",MUTED)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 5 — SUBTITLE EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════
class SubtitleTab(tk.Frame):
    def __init__(self,parent):
        super().__init__(parent,bg=BG); self.save_dir=get_save_dir(); self._build()
    def _build(self):
        inner=make_scrollable(self); c=CardFrame(inner); c.pack(fill="x",padx=14,pady=(12,4))
        tk.Label(c,text="📄 Subtitle Extractor",font=("Helvetica",13,"bold"),fg=WHITE,bg=BG_CARD).pack(anchor="w")
        tk.Label(c,text="Embedded ya auto-generated subtitles — SRT, VTT, TXT",font=("Helvetica",8),fg=MUTED,bg=BG_CARD).pack(anchor="w",pady=(2,10))
        Hdr(c,"URL:").pack(anchor="w")
        ur=tk.Frame(c,bg=BG_CARD); ur.pack(fill="x",pady=(4,10))
        self.url_var=tk.StringVar()
        Inp(ur,var=self.url_var).pack(side="left",fill="x",expand=True,ipady=7,ipadx=8)
        Btn(ur,"Paste",lambda: self.url_var.set(self.winfo_toplevel().clipboard_get().strip()),bg=BG_INPUT,fg=MUTED,px=8).pack(side="left",padx=6)
        Btn(ur,"🔍 Fetch Languages",self._fetch_langs,bg=YELLOW,fg="#000",px=10).pack(side="left",padx=4)
        r2=tk.Frame(c,bg=BG_CARD); r2.pack(fill="x",pady=(0,10))
        Hdr(r2,"Language:").pack(side="left")
        self.lang_var=tk.StringVar(value="en")
        self.lang_combo=mk_combo(r2,self.lang_var,["en","hi","ur","zh","ar","fr","de","es","ja","ko","pt","ru","auto"],w=14)
        self.lang_combo.pack(side="left",padx=(6,20))
        Hdr(r2,"Format:").pack(side="left")
        self.sfmt_var=tk.StringVar(value="srt")
        for fmt in ["srt","vtt","txt","ass"]:
            tk.Radiobutton(r2,text=fmt.upper(),variable=self.sfmt_var,value=fmt,
                bg=BG_CARD,fg=WHITE,selectcolor=ACCENT,activebackground=BG_CARD,font=("Helvetica",9)).pack(side="left",padx=4)
        op=tk.Frame(c,bg=BG_CARD); op.pack(fill="x",pady=(0,10))
        self.auto_var=tk.BooleanVar(value=True); self.manual_var=tk.BooleanVar(value=True)
        for txt,var in [("Auto-generated",self.auto_var),("Manual/Embedded",self.manual_var)]:
            tk.Checkbutton(op,text=txt,variable=var,bg=BG_CARD,fg=WHITE,selectcolor=ACCENT,
                activebackground=BG_CARD,font=("Helvetica",9)).pack(side="left",padx=(0,18))
        dr=tk.Frame(c,bg=BG_CARD); dr.pack(fill="x",pady=(0,8))
        Hdr(dr,"Save To:").pack(side="left")
        self.dir_lbl=tk.Label(dr,text=self.save_dir,font=("Helvetica",9),fg=MUTED,bg=BG_CARD,anchor="w",wraplength=480)
        self.dir_lbl.pack(side="left",padx=6,fill="x",expand=True)
        Btn(dr,"📂",self._browse_sub,bg=BG_INPUT,fg=MUTED,fs=9,py=4,px=8).pack(side="right")
        Btn(c,"📄  Download Subtitles",self._start,bg=GREEN,fg=WHITE,fs=12,py=9).pack(fill="x",pady=(4,8))
        Hdr(c,"Available Subtitle Tracks (after fetch):").pack(anchor="w")
        self.subs_box=scrolledtext.ScrolledText(c,height=8,bg=BG_INPUT,fg=WHITE,font=("Courier",9),relief="flat",state="disabled")
        self.subs_box.pack(fill="x",pady=(4,0))
        self.st_lbl=tk.Label(inner,text="Ready",font=("Helvetica",9),fg=MUTED,bg=BG)
        self.st_lbl.pack(anchor="e",padx=14,pady=4)
        add_disclaimer(inner)
    def _browse_sub(self):
        d = filedialog.askdirectory(initialdir=self.save_dir)
        if d:
            self.save_dir = d
            self.dir_lbl.config(text=d)
    def _set_st(self,msg,c=MUTED):
        self.st_lbl.config(text=msg,fg=c); self.winfo_toplevel().update_idletasks()
    def _fetch_langs(self):
        url=self.url_var.get().strip()
        if not url: messagebox.showwarning("FastTube Pro","URL daalo!"); return
        self._set_st("Fetching subtitle tracks…",YELLOW)
        threading.Thread(target=self._fetch_th,args=(url,),daemon=True).start()
    def _fetch_th(self,url):
        pkey=detect_platform(url)
        try:
            o=base_ydl_opts(pkey); o["skip_download"]=True
            with yt_dlp.YoutubeDL(o) as ydl: info=ydl.extract_info(url,download=False)
            subs=info.get("subtitles",{}); auto=info.get("automatic_captions",{})
            lines=[]
            if subs: lines.append("=== MANUAL SUBTITLES ===")
            for lang,data in subs.items():
                fmts=[d.get("ext","?") for d in data]; lines.append(f"  {lang}: {', '.join(fmts)}")
            if auto:
                lines.append("\n=== AUTO-GENERATED ===")
                for lang in list(auto.keys())[:40]: lines.append(f"  {lang}")
            if not lines: lines=["No subtitles found for this video."]
            all_langs=list(subs.keys())+list(auto.keys())
            self.after(0,lambda ls=all_langs,txt="\n".join(lines): self._show_subs(ls,txt))
        except Exception as e:
            self.after(0,lambda m=str(e): self._set_st(f"❌ {strip_ansi(m)[:60]}",ACCENT))
    def _show_subs(self,langs,text):
        self.subs_box.config(state="normal"); self.subs_box.delete("1.0","end")
        self.subs_box.insert("end",text); self.subs_box.config(state="disabled")
        if langs: self.lang_combo.config(values=langs); self.lang_var.set(langs[0])
        self._set_st(f"✅ {len(langs)} tracks found",GREEN)
    def _start(self):
        ok, msg, _, _ = sub_check_feature("subtitles")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return
        url=self.url_var.get().strip()
        if not url: messagebox.showwarning("FastTube Pro","URL daalo!"); return

        # ── Duplicate check ──────────────────────────────────────────────────
        is_dup, dup_row = db_check_duplicate(url)
        if is_dup and not ask_duplicate_popup(self.winfo_toplevel(), url, dup_row):
            return

        self._set_st("Downloading subtitles…",YELLOW)
        threading.Thread(target=self._run,daemon=True).start()
    def _run(self):
        url=self.url_var.get().strip(); lang=self.lang_var.get().strip() or "en"; sfmt=self.sfmt_var.get()
        pkey=detect_platform(url)
        opts=base_ydl_opts(pkey)
        opts.update({"skip_download":True,"writesubtitles":self.manual_var.get(),
            "writeautomaticsub":self.auto_var.get(),"subtitleslangs":[lang],
            "subtitlesformat":sfmt,"outtmpl":os.path.join(self.save_dir,"%(title)s.%(ext)s")})
        try:
            with yt_dlp.YoutubeDL(opts) as ydl: ydl.download([url])
            sub_record_feature("subtitles")
            self.after(0,lambda: (self._set_st("✅ Subtitles downloaded!",GREEN),
                messagebox.showinfo("FastTube Pro",f"✅ Saved!\n{self.save_dir}")))
        except Exception as e:
            err=strip_ansi(str(e))[:100]
            self.after(0,lambda m=err: (self._set_st(f"❌ {m}",ACCENT),messagebox.showerror("Error",m)))


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 6 — METADATA VIEWER
# ══════════════════════════════════════════════════════════════════════════════
class MetadataTab(tk.Frame):
    """
    Metadata Studio — 2 inner tabs:
      • Export  : Fetch metadata → view all fields → export JSON/CSV/TXT
      • Compare : Fetch 2 URLs → side-by-side diff
    """

    # ── All fields we extract ────────────────────────────────────────────────
    FIELDS = [
        ("Title",           "title"),
        ("Channel",         "uploader"),
        ("Duration",        "dur_s"),
        ("Views",           "view_count"),
        ("Likes",           "like_count"),
        ("Upload Date",     "upload_date"),
        ("Best Resolution", "res"),
        ("FPS",             "fps"),
        ("Video Codec",     "vcodec"),
        ("Audio Codec",     "acodec"),
        ("Avg Bitrate",     "tbr"),
        ("File Size",       "fs_s"),
        ("Platform",        "pname"),
        ("Age Limit",       "age_limit"),
        ("Language",        "language"),
        ("Categories",      "categories"),
        ("Tags",            "tags"),
        ("Subtitles",       "subs"),
        ("Description",     "desc"),
        ("URL",             "webpage_url"),
    ]

    def __init__(self, parent):
        super().__init__(parent, bg=BG)
        # Export tab state
        self._info_a    = None
        self._thumb_url = ""
        # Compare tab state
        self._info_l    = None
        self._info_r    = None
        self._build()

    # ════════════════════════════════════════════════════════════════════════
    #  TOP-LEVEL BUILD
    # ════════════════════════════════════════════════════════════════════════
    def _build(self):
        # Header row
        hdr = tk.Frame(self, bg=BG, pady=8)
        hdr.pack(fill="x", padx=14)
        tk.Label(hdr, text="🎛  Metadata Studio",
                 font=("Helvetica", 15, "bold"),
                 fg=WHITE, bg=BG).pack(side="left")
        tk.Label(hdr, text="Export · Compare",
                 font=("Helvetica", 9), fg=MUTED, bg=BG).pack(
                 side="left", padx=(10, 0), pady=(4, 0))

        tk.Frame(self, bg=BORDER, height=1).pack(fill="x", padx=14)

        # Inner notebook (2 tabs)
        s = ttk.Style()
        s.configure("MS.TNotebook",
                    background=BG, bordercolor=BG, tabmargins=[0, 4, 0, 0])
        s.configure("MS.TNotebook.Tab",
                    background=BG_INPUT, foreground=MUTED,
                    font=("Helvetica", 9, "bold"), padding=[16, 6],
                    bordercolor=BG)
        s.map("MS.TNotebook.Tab",
              background=[("selected", ACCENT)],
              foreground=[("selected", WHITE)])

        nb = ttk.Notebook(self, style="MS.TNotebook")
        nb.pack(fill="both", expand=True, padx=14, pady=8)

        # Tab A — Export
        exp_frame = tk.Frame(nb, bg=BG)
        nb.add(exp_frame, text="  📤  Export  ")
        self._build_export(exp_frame)

        # Tab B — Compare
        cmp_frame = tk.Frame(nb, bg=BG)
        nb.add(cmp_frame, text="  ⚖  Compare  ")
        self._build_compare(cmp_frame)

        # Disclaimer at bottom of MetadataTab (outside notebook)
        disc_inner = tk.Frame(self, bg=BG)
        disc_inner.pack(fill="x")
        add_disclaimer(disc_inner)

    # ════════════════════════════════════════════════════════════════════════
    #  TAB A — EXPORT
    # ════════════════════════════════════════════════════════════════════════
    def _build_export(self, parent):
        top = CardFrame(parent)
        top.pack(fill="x", padx=0, pady=(8, 6))

        tk.Label(top, text="📤  Export Metadata",
                 font=("Helvetica", 12, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(top,
                 text="URL paste → Fetch → Export as JSON / CSV / TXT",
                 font=("Helvetica", 8), fg=MUTED,
                 bg=BG_CARD).pack(anchor="w", pady=(2, 10))

        # URL row
        ur = tk.Frame(top, bg=BG_CARD)
        ur.pack(fill="x", pady=(0, 8))
        self.exp_url = tk.StringVar()
        Inp(ur, var=self.exp_url).pack(
            side="left", fill="x", expand=True, ipady=7, ipadx=8)
        Btn(ur, "Paste", self._exp_paste,
            bg=BG_INPUT, fg=MUTED, px=8).pack(side="left", padx=(6, 0))
        Btn(ur, "Clear", self._exp_clear,
            bg=BG_INPUT, fg=MUTED, px=8).pack(side="left", padx=(4, 0))
        self.exp_fetch_btn = Btn(ur, "🔍  Fetch",
                                  self._exp_fetch,
                                  bg=ACCENT, fg=WHITE, px=14)
        self.exp_fetch_btn.pack(side="left", padx=(8, 0))

        # Status
        self.exp_st = tk.Label(top, text="Paste a URL and click Fetch",
                                font=("Helvetica", 8), fg=MUTED, bg=BG_CARD)
        self.exp_st.pack(anchor="w", pady=(0, 4))

        # ── Split: fields (left) + thumbnail (right) ─────────────────────
        split = tk.Frame(parent, bg=BG)
        split.pack(fill="both", expand=True, pady=(0, 6))

        # Left — scrollable fields
        left = tk.Frame(split, bg=BG)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        fields_inner = make_scrollable(left)
        fields_card  = CardFrame(fields_inner)
        fields_card.pack(fill="x", padx=0, pady=0)

        self.exp_lbls = {}
        for fname, fkey in self.FIELDS:
            row = tk.Frame(fields_card, bg=BG_CARD)
            row.pack(fill="x", pady=2)
            tk.Label(row,
                     text=f"{fname}:",
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG_CARD,
                     width=16, anchor="e").pack(side="left")
            val = tk.Label(row, text="—",
                           font=("Helvetica", 8),
                           fg=WHITE, bg=BG_CARD,
                           anchor="w", wraplength=420, justify="left")
            val.pack(side="left", padx=(8, 0), fill="x", expand=True)
            self.exp_lbls[fkey] = val

        # Right — thumbnail + export buttons
        right = tk.Frame(split, bg=BG_CARD, width=220, padx=12, pady=12)
        right.pack(side="right", fill="y")
        right.pack_propagate(False)

        tk.Label(right, text="Thumbnail",
                 font=("Helvetica", 9, "bold"),
                 fg=MUTED, bg=BG_CARD).pack()
        self.exp_thumb = tk.Label(
            right, text="No thumbnail",
            font=("Helvetica", 8), fg=DIM, bg=BG_CARD,
            width=26, height=9)
        self.exp_thumb.pack(pady=(4, 6))

        tk.Frame(right, bg=BORDER, height=1).pack(fill="x", pady=(0, 8))

        tk.Label(right, text="Export As:",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w")

        for lbl, cmd in [
            ("💾  Save JSON", self._export_json),
            ("📊  Save CSV",  self._export_csv),
            ("📄  Save TXT",  self._export_txt),
        ]:
            Btn(right, lbl, cmd,
                bg=BG_INPUT, fg=WHITE, fs=9, py=7
                ).pack(fill="x", pady=(4, 0))

        Btn(right, "🖼  Save Thumbnail",
            self._save_thumb,
            bg=DIM, fg=MUTED, fs=8, py=6
            ).pack(fill="x", pady=(10, 0))

    # ── Export helpers ────────────────────────────────────────────────────
    def _exp_paste(self):
        try:
            self.exp_url.set(
                self.winfo_toplevel().clipboard_get().strip())
        except Exception:
            pass

    def _exp_clear(self):
        self.exp_url.set("")
        for v in self.exp_lbls.values():
            v.config(text="—", fg=WHITE)
        self.exp_thumb.config(image="", text="No thumbnail",
                               width=26, height=9)
        self.exp_st.config(text="Paste a URL and click Fetch", fg=MUTED)
        self._info_a    = None
        self._thumb_url = ""

    def _exp_fetch(self):
        ok, msg, _, _ = sub_check_feature("metadata")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return
        url = self.exp_url.get().strip()
        if not url:
            messagebox.showwarning("Metadata Studio", "URL daalo!")
            return
        self.exp_st.config(text="⏳ Fetching…", fg=YELLOW)
        self.exp_fetch_btn.config(state="disabled", text="Fetching…")
        threading.Thread(target=self._exp_fetch_th,
                         args=(url,), daemon=True).start()

    def _exp_fetch_th(self, url):
        pkey = detect_platform(url)
        try:
            if pkey == "mxplayer":
                info = mx_fetch_info(url)
            else:
                o = base_ydl_opts(pkey)
                o["skip_download"] = True
                with yt_dlp.YoutubeDL(o) as ydl:
                    info = ydl.extract_info(url, download=False)
            self._info_a = info
            self.after(0, lambda: self._exp_show(info))
        except Exception as e:
            err = strip_ansi(str(e))[:100]
            self.after(0, lambda m=err: (
                self.exp_st.config(text=f"❌ {m}", fg=ACCENT),
                self.exp_fetch_btn.config(state="normal",
                                          text="🔍  Fetch")))

    def _exp_show(self, info):
        data = self._extract_fields(info)
        for k, v in data.items():
            if k in self.exp_lbls:
                self.exp_lbls[k].config(text=str(v) or "—")
        # Thumbnail
        thumbs = info.get("thumbnails", []) or []
        if thumbs:
            bt = max(thumbs,
                     key=lambda t: (t.get("width", 0) or 0) *
                                   (t.get("height", 0) or 0))
            self._thumb_url = bt.get("url", "")
            threading.Thread(target=self._load_thumb,
                             args=(self._thumb_url, self.exp_thumb),
                             daemon=True).start()
        self.exp_st.config(text="✅ Metadata loaded", fg=GREEN)
        self.exp_fetch_btn.config(state="normal", text="🔍  Fetch")

    def _extract_fields(self, info):
        dur  = info.get("duration", 0) or 0
        dur_s = (f"{int(dur//3600)}h {int((dur%3600)//60)}m "
                 f"{int(dur%60)}s" if dur else "—")
        fs   = info.get("filesize") or info.get("filesize_approx", 0)
        fs_s = f"{fs/1024/1024:.1f} MB" if fs else "—"
        ud   = info.get("upload_date", "") or ""
        if len(ud) == 8:
            ud = f"{ud[:4]}-{ud[4:6]}-{ud[6:]}"
        fmts = info.get("formats", []) or []
        best = (max(fmts, key=lambda f: (f.get("height", 0) or 0))
                if fmts else {})
        res  = (f"{best.get('width','?')}×{best.get('height','?')}"
                if best else "—")
        pkey = detect_platform(info.get("webpage_url", ""))
        return {
            "title":       info.get("title", "—") or "—",
            "uploader":    (info.get("uploader") or
                            info.get("channel") or "—"),
            "dur_s":       dur_s,
            "view_count":  (f"{info.get('view_count',0):,}"
                            if info.get("view_count") else "—"),
            "like_count":  (f"{info.get('like_count',0):,}"
                            if info.get("like_count") else "—"),
            "upload_date": ud or "—",
            "res":         res,
            "fps":         str(info.get("fps", "—") or "—"),
            "vcodec":      info.get("vcodec", "—") or "—",
            "acodec":      info.get("acodec", "—") or "—",
            "tbr":         (f"{info.get('tbr','—')} kbps"
                            if info.get("tbr") else "—"),
            "fs_s":        fs_s,
            "pname":       PLATFORMS.get(pkey, {}).get("name", "Other"),
            "age_limit":   str(info.get("age_limit", 0) or 0),
            "language":    info.get("language", "—") or "—",
            "categories":  ", ".join(
                               (info.get("categories") or [])[:5]) or "—",
            "tags":        ", ".join(
                               (info.get("tags") or [])[:8]) or "—",
            "subs":        ", ".join(
                               list((info.get("subtitles") or
                                    {}).keys())[:6]) or "None",
            "desc":        (info.get("description") or "")[:220] or "—",
            "webpage_url": info.get("webpage_url", "—") or "—",
        }

    def _load_thumb(self, url, lbl):
        try:
            req  = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"})
            data = urllib.request.urlopen(req, timeout=10).read()
            try:
                from PIL import Image, ImageTk
                from io import BytesIO
                img   = Image.open(BytesIO(data))
                img   = img.resize((200, 112), Image.LANCZOS)
                photo = ImageTk.PhotoImage(img)
                self.after(0, lambda p=photo, w=lbl: (
                    w.config(image=p, text="",
                             width=200, height=112),
                    setattr(w, "_photo", p)))
            except ImportError:
                self.after(0, lambda w=lbl: w.config(
                    text="Found\n(pip install pillow\nfor preview)"))
        except Exception:
            self.after(0, lambda w=lbl:
                       w.config(text="Preview unavailable"))

    # ── Export to file ────────────────────────────────────────────────────
    def _export_json(self):
        if not self._info_a:
            messagebox.showwarning("Metadata Studio",
                                   "Pehle Fetch karo!"); return
        import json
        data = self._extract_fields(self._info_a)
        f = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("All", "*.*")],
            initialdir=get_save_dir(),
            initialfile="metadata.json")
        if not f: return
        with open(f, "w", encoding="utf-8") as fp:
            json.dump(data, fp, indent=2, ensure_ascii=False)
        messagebox.showinfo("Metadata Studio",
                            f"✅ JSON saved:\n{f}")

    def _export_csv(self):
        if not self._info_a:
            messagebox.showwarning("Metadata Studio",
                                   "Pehle Fetch karo!"); return
        import csv
        data = self._extract_fields(self._info_a)
        f = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All", "*.*")],
            initialdir=get_save_dir(),
            initialfile="metadata.csv")
        if not f: return
        with open(f, "w", newline="", encoding="utf-8") as fp:
            w = csv.writer(fp)
            w.writerow(["Field", "Value"])
            for label, key in self.FIELDS:
                w.writerow([label, data.get(key, "—")])
        messagebox.showinfo("Metadata Studio",
                            f"✅ CSV saved:\n{f}")

    def _export_txt(self):
        if not self._info_a:
            messagebox.showwarning("Metadata Studio",
                                   "Pehle Fetch karo!"); return
        data = self._extract_fields(self._info_a)
        f = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text", "*.txt"), ("All", "*.*")],
            initialdir=get_save_dir(),
            initialfile="metadata.txt")
        if not f: return
        lines = ["=" * 54, "  METADATA STUDIO — FastTube Pro", "=" * 54, ""]
        for label, key in self.FIELDS:
            lines.append(f"{label:<18}: {data.get(key, '—')}")
        lines += ["", "=" * 54]
        with open(f, "w", encoding="utf-8") as fp:
            fp.write("\n".join(lines))
        messagebox.showinfo("Metadata Studio",
                            f"✅ TXT saved:\n{f}")

    def _save_thumb(self):
        if not self._thumb_url:
            messagebox.showwarning("Metadata Studio",
                                   "Pehle Fetch karo!"); return
        f = filedialog.asksaveasfilename(
            defaultextension=".jpg",
            filetypes=[("JPEG", "*.jpg"), ("PNG", "*.png"),
                       ("All", "*.*")],
            initialdir=get_save_dir())
        if not f: return
        try:
            req  = urllib.request.Request(
                self._thumb_url,
                headers={"User-Agent": "Mozilla/5.0"})
            data = urllib.request.urlopen(req, timeout=10).read()
            with open(f, "wb") as fp:
                fp.write(data)
            messagebox.showinfo("Metadata Studio",
                                f"✅ Thumbnail saved:\n{f}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # ════════════════════════════════════════════════════════════════════════
    #  TAB B — COMPARE
    # ════════════════════════════════════════════════════════════════════════
    def _build_compare(self, parent):
        top = CardFrame(parent)
        top.pack(fill="x", padx=0, pady=(8, 6))

        tk.Label(top, text="⚖  Compare Two Videos",
                 font=("Helvetica", 12, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(top,
                 text="Paste 2 URLs → Fetch Both → Side-by-side comparison",
                 font=("Helvetica", 8),
                 fg=MUTED, bg=BG_CARD).pack(anchor="w", pady=(2, 10))

        # ── URL inputs row ────────────────────────────────────────────────
        urls_row = tk.Frame(top, bg=BG_CARD)
        urls_row.pack(fill="x", pady=(0, 6))

        # Left URL
        lf = tk.Frame(urls_row, bg=BG_CARD)
        lf.pack(side="left", fill="x", expand=True, padx=(0, 8))
        tk.Label(lf, text="Video A:",
                 font=("Helvetica", 8, "bold"),
                 fg=BLUE, bg=BG_CARD).pack(anchor="w")
        ur_l = tk.Frame(lf, bg=BG_CARD)
        ur_l.pack(fill="x", pady=(2, 0))
        self.cmp_url_l = tk.StringVar()
        Inp(ur_l, var=self.cmp_url_l).pack(
            side="left", fill="x", expand=True, ipady=6, ipadx=6)
        Btn(ur_l, "Paste",
            lambda: self.cmp_url_l.set(
                self.winfo_toplevel().clipboard_get().strip()),
            bg=BG_INPUT, fg=MUTED, fs=8, px=6, py=4).pack(
            side="left", padx=(4, 0))

        # Right URL
        rf = tk.Frame(urls_row, bg=BG_CARD)
        rf.pack(side="left", fill="x", expand=True)
        tk.Label(rf, text="Video B:",
                 font=("Helvetica", 8, "bold"),
                 fg=GREEN, bg=BG_CARD).pack(anchor="w")
        ur_r = tk.Frame(rf, bg=BG_CARD)
        ur_r.pack(fill="x", pady=(2, 0))
        self.cmp_url_r = tk.StringVar()
        Inp(ur_r, var=self.cmp_url_r).pack(
            side="left", fill="x", expand=True, ipady=6, ipadx=6)
        Btn(ur_r, "Paste",
            lambda: self.cmp_url_r.set(
                self.winfo_toplevel().clipboard_get().strip()),
            bg=BG_INPUT, fg=MUTED, fs=8, px=6, py=4).pack(
            side="left", padx=(4, 0))

        # Fetch + Clear buttons
        btn_row = tk.Frame(top, bg=BG_CARD)
        btn_row.pack(fill="x", pady=(6, 0))
        self.cmp_fetch_btn = Btn(btn_row,
                                  "⚖  Fetch & Compare Both",
                                  self._cmp_fetch,
                                  bg=ACCENT, fg=WHITE, fs=11, py=8)
        self.cmp_fetch_btn.pack(side="left", fill="x",
                                 expand=True, padx=(0, 8))
        Btn(btn_row, "🗑 Clear", self._cmp_clear,
            bg=BG_INPUT, fg=MUTED, fs=9, py=8).pack(side="left")

        self.cmp_st = tk.Label(top, text="",
                                font=("Helvetica", 8),
                                fg=MUTED, bg=BG_CARD)
        self.cmp_st.pack(anchor="w", pady=(4, 0))

        # ── Comparison table ──────────────────────────────────────────────
        tbl_frame = tk.Frame(parent, bg=BG)
        tbl_frame.pack(fill="both", expand=True, pady=(0, 6))

        tv_style()
        cols   = ("Field", "Video A", "Video B", "Diff")
        widths = [140, 260, 260, 80]
        tbl_inner = make_scrollable(tbl_frame)
        tbl_card  = CardFrame(tbl_inner)
        tbl_card.pack(fill="x")

        tbl_f = tk.Frame(tbl_card, bg=BG_CARD)
        tbl_f.pack(fill="x")
        self.cmp_tv = ttk.Treeview(tbl_f, columns=cols,
                                    show="headings", height=18)
        for col, w in zip(cols, widths):
            self.cmp_tv.heading(col, text=col)
            self.cmp_tv.column(col, width=w, minwidth=40,
                                anchor="w" if col != "Diff" else "center")
        vsb = ttk.Scrollbar(tbl_f, orient="vertical",
                             command=self.cmp_tv.yview)
        self.cmp_tv.configure(yscrollcommand=vsb.set)
        self.cmp_tv.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Row color tags
        self.cmp_tv.tag_configure("diff",  foreground=YELLOW,
                                   background="#2a2000")
        self.cmp_tv.tag_configure("same",  foreground=MUTED)
        self.cmp_tv.tag_configure("field", foreground=WHITE,
                                   font=("Helvetica", 8, "bold"))
        self.cmp_tv.tag_configure("head",  foreground=ACCENT,
                                   background=BG_INPUT)

        # Thumbnails row below table
        thumb_row = tk.Frame(parent, bg=BG)
        thumb_row.pack(fill="x", padx=0, pady=(0, 4))

        for side, attr, col in [
            ("left",  "cmp_thumb_l", BLUE),
            ("right", "cmp_thumb_r", GREEN),
        ]:
            tc = tk.Frame(thumb_row, bg=BG_CARD,
                          padx=10, pady=8)
            tc.pack(side=side, fill="x", expand=True,
                    padx=(0, 6) if side == "left" else (6, 0))
            lbl_text = "Video A Thumbnail" if side == "left" else "Video B Thumbnail"
            tk.Label(tc, text=lbl_text,
                     font=("Helvetica", 8, "bold"),
                     fg=col, bg=BG_CARD).pack()
            img_lbl = tk.Label(tc,
                                text="—",
                                font=("Helvetica", 8),
                                fg=DIM, bg=BG_CARD,
                                width=28, height=7)
            img_lbl.pack(pady=(4, 0))
            setattr(self, attr, img_lbl)

    # ── Compare helpers ───────────────────────────────────────────────────
    def _cmp_clear(self):
        self.cmp_url_l.set("")
        self.cmp_url_r.set("")
        for i in self.cmp_tv.get_children():
            self.cmp_tv.delete(i)
        self.cmp_thumb_l.config(image="", text="—", width=28, height=7)
        self.cmp_thumb_r.config(image="", text="—", width=28, height=7)
        self.cmp_st.config(text="", fg=MUTED)
        self._info_l = None
        self._info_r = None

    def _cmp_fetch(self):
        ul = self.cmp_url_l.get().strip()
        ur = self.cmp_url_r.get().strip()
        if not ul or not ur:
            messagebox.showwarning("Metadata Studio",
                                   "Dono URLs daalo!"); return
        self.cmp_st.config(text="⏳ Fetching both…", fg=YELLOW)
        self.cmp_fetch_btn.config(state="disabled",
                                   text="Fetching…")
        threading.Thread(target=self._cmp_fetch_th,
                         args=(ul, ur), daemon=True).start()

    def _cmp_fetch_th(self, ul, ur):
        results = {}
        for side, url in [("left", ul), ("right", ur)]:
            pkey = detect_platform(url)
            try:
                if pkey == "mxplayer":
                    info = mx_fetch_info(url)
                else:
                    o = base_ydl_opts(pkey)
                    o["skip_download"] = True
                    with yt_dlp.YoutubeDL(o) as ydl:
                        info = ydl.extract_info(url, download=False)
                results[side] = info
            except Exception as e:
                err = strip_ansi(str(e))[:80]
                self.after(0, lambda m=err, s=side:
                           self.cmp_st.config(
                               text=f"❌ {s.upper()} failed: {m}",
                               fg=ACCENT))
                self.after(0, lambda:
                           self.cmp_fetch_btn.config(
                               state="normal",
                               text="⚖  Fetch & Compare Both"))
                return

        self._info_l = results.get("left")
        self._info_r = results.get("right")
        self.after(0, lambda: self._cmp_show(
            self._info_l, self._info_r))

    def _cmp_show(self, il, ir):
        for i in self.cmp_tv.get_children():
            self.cmp_tv.delete(i)

        dl = self._extract_fields(il)
        dr = self._extract_fields(ir)

        COMPARE_FIELDS = [
            ("Title",        "title"),
            ("Channel",      "uploader"),
            ("Duration",     "dur_s"),
            ("Views",        "view_count"),
            ("Likes",        "like_count"),
            ("Upload Date",  "upload_date"),
            ("Resolution",   "res"),
            ("FPS",          "fps"),
            ("Video Codec",  "vcodec"),
            ("Audio Codec",  "acodec"),
            ("Avg Bitrate",  "tbr"),
            ("File Size",    "fs_s"),
            ("Platform",     "pname"),
            ("Language",     "language"),
            ("Age Limit",    "age_limit"),
            ("Subtitles",    "subs"),
            ("Tags",         "tags"),
        ]

        diffs = 0
        for label, key in COMPARE_FIELDS:
            va = str(dl.get(key, "—"))
            vb = str(dr.get(key, "—"))
            is_diff = (va != vb and va != "—" and vb != "—")
            if is_diff:
                diffs += 1
            tag  = "diff" if is_diff else "same"
            diff_sym = "≠" if is_diff else "="
            self.cmp_tv.insert("", "end",
                values=(label, va[:58], vb[:58], diff_sym),
                tags=(tag,))

        # Load thumbnails
        for info, attr in [(il, "cmp_thumb_l"), (ir, "cmp_thumb_r")]:
            thumbs = info.get("thumbnails", []) or []
            if thumbs:
                bt = max(thumbs,
                         key=lambda t: (t.get("width", 0) or 0) *
                                       (t.get("height", 0) or 0))
                turl = bt.get("url", "")
                lbl  = getattr(self, attr)
                threading.Thread(target=self._load_thumb,
                                 args=(turl, lbl),
                                 daemon=True).start()

        total = len(COMPARE_FIELDS)
        self.cmp_st.config(
            text=(f"✅ Compare done — {diffs}/{total} fields differ"
                  if diffs else
                  f"✅ Compare done — All {total} fields match"),
            fg=YELLOW if diffs else GREEN)
        self.cmp_fetch_btn.config(state="normal",
                                   text="⚖  Fetch & Compare Both")



# ══════════════════════════════════════════════════════════════════════════════
#  TAB 7 — THUMBNAIL DOWNLOADER
#  FIX: Uses urllib to download image directly as .jpg — NOT yt-dlp download
#       (yt-dlp would download the video; we only want the image file)
# ══════════════════════════════════════════════════════════════════════════════
class ThumbnailTab(tk.Frame):

    YT_QUALITIES = [
        ("maxresdefault — 1280×720", "maxresdefault"),
        ("sddefault    — 640×480",   "sddefault"),
        ("hqdefault    — 480×360",   "hqdefault"),
        ("mqdefault    — 320×180",   "mqdefault"),
        ("default      — 120×90",    "default"),
    ]

    def __init__(self, parent):
        super().__init__(parent, bg=BG)
        self.save_dir = get_save_dir()
        self._cancel  = False
        self._build()

    def _build(self):
        inner = make_scrollable(self)
        c = CardFrame(inner); c.pack(fill="x", padx=14, pady=(12, 4))

        tk.Label(c, text="🖼 Thumbnail Downloader — YouTube Only",
                 font=("Helvetica",13,"bold"), fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(c, text="Highest resolution YouTube thumbnail — single ya bulk (.jpg file, VIDEO nahi)",
                 font=("Helvetica",8), fg=MUTED, bg=BG_CARD).pack(anchor="w", pady=(2,6))

        # Platform support — YouTube only
        plat_row = tk.Frame(c, bg=BG_CARD)
        plat_row.pack(anchor="w", pady=(0,10))
        tk.Label(plat_row, text="Works with:",
                 font=("Helvetica",7,"bold"),
                 fg=MUTED, bg=BG_CARD).pack(side="left", padx=(0,6))
        tk.Label(plat_row, text="▶ YouTube",
                 font=("Helvetica",7,"bold"),
                 fg="#e53935", bg=BG_CARD, padx=4).pack(side="left")

        Hdr(c,"Single URL:").pack(anchor="w")
        ur = tk.Frame(c, bg=BG_CARD); ur.pack(fill="x", pady=(4,8))
        self.url_var = tk.StringVar()
        Inp(ur, var=self.url_var).pack(side="left",fill="x",expand=True,ipady=7,ipadx=8)
        Btn(ur,"Paste",lambda: self.url_var.set(self.winfo_toplevel().clipboard_get().strip()),
            bg=BG_INPUT,fg=MUTED,px=8).pack(side="left",padx=6)

        Hdr(c,"Bulk URLs (one per line):").pack(anchor="w")
        self.bulk_box = scrolledtext.ScrolledText(
            c, height=5, bg=BG_INPUT, fg=WHITE,
            insertbackground=WHITE, font=("Helvetica",10),
            relief="flat", highlightthickness=1,
            highlightbackground=BORDER, highlightcolor=ACCENT)
        self.bulk_box.pack(fill="x", pady=(4,10))

        # Quality row (YouTube specific)
        r2 = tk.Frame(c, bg=BG_CARD); r2.pack(fill="x", pady=(0,8))
        Hdr(r2,"YouTube Quality:").pack(side="left")
        self.yt_q_var = tk.StringVar(value="maxresdefault — 1280×720")
        mk_combo(r2, self.yt_q_var, [q[0] for q in self.YT_QUALITIES],
                 w=28).pack(side="left",padx=(6,0))

        # Filename prefix
        r3 = tk.Frame(c, bg=BG_CARD); r3.pack(fill="x", pady=(0,8))
        Hdr(r3,"Custom prefix (optional):").pack(side="left")
        self.prefix_var = tk.StringVar()
        Inp(r3, var=self.prefix_var, w=20).pack(side="left",padx=(6,0),ipady=4)

        dr = tk.Frame(c, bg=BG_CARD); dr.pack(fill="x", pady=(0,8))
        Hdr(dr,"Save To:").pack(side="left")
        self.dir_lbl = tk.Label(dr, text=self.save_dir,
                                font=("Helvetica",9), fg=MUTED, bg=BG_CARD,
                                anchor="w", wraplength=510)
        self.dir_lbl.pack(side="left",padx=6,fill="x",expand=True)
        Btn(dr,"📂",self._browse,bg=BG_INPUT,fg=MUTED,fs=9,py=4,px=8).pack(side="right")

        ctrl = tk.Frame(c, bg=BG_CARD); ctrl.pack(fill="x",pady=(4,8))
        self.dl_btn = Btn(ctrl,"🖼  Download Thumbnails",self._start,
                          bg=ACCENT,fg=WHITE,fs=12,py=9)
        self.dl_btn.pack(side="left",fill="x",expand=True,padx=(0,6))
        self.cx_btn = Btn(ctrl,"✖  Cancel",self._do_cancel,
                          bg=DIM,fg=MUTED,fs=11,py=9)
        self.cx_btn.pack(side="left"); self.cx_btn.config(state="disabled")

        # Results
        rc = CardFrame(inner); rc.pack(fill="x",padx=14,pady=(4,12))
        self.pb  = mk_pb(rc); self.pb.pack(fill="x",pady=(0,6))
        self.log = scrolledtext.ScrolledText(rc,height=8,bg=BG_INPUT,
            fg=GREEN,font=("Courier",8),relief="flat",state="disabled")
        self.log.pack(fill="x")
        bs = tk.Frame(rc,bg=BG_CARD); bs.pack(fill="x",pady=(6,0))
        self.st_lbl  = tk.Label(bs,text="Ready",font=("Helvetica",9),
                                fg=MUTED,bg=BG_CARD,anchor="e")
        self.st_lbl.pack(side="right")
        self.cnt_lbl = tk.Label(bs,text="",font=("Helvetica",9,"bold"),
                                fg=GREEN,bg=BG_CARD,anchor="w")
        self.cnt_lbl.pack(side="left")

        add_disclaimer(inner)

    def _pick_best_thumb(self, thumbs, info, pkey, yt_q_key):
        """Platform-aware: returns best image URL or None."""
        # YouTube CDN direct
        if pkey == "youtube":
            vid_id = info.get("id", "")
            if vid_id:
                for qk in [yt_q_key, "sddefault", "hqdefault", "mqdefault", "default"]:
                    test = "https://img.youtube.com/vi/{}/{}.jpg".format(vid_id, qk)
                    try:
                        req  = urllib.request.Request(
                            test, headers={"User-Agent": "Mozilla/5.0"})
                        resp = urllib.request.urlopen(req, timeout=8)
                        if int(resp.headers.get("Content-Length", 99999)) > 2000:
                            return test
                    except Exception:
                        continue

        # Filter by platform-specific CDN
        if thumbs:
            if pkey == "instagram":
                cand = [t for t in thumbs
                        if "scontent" in t.get("url","")
                        or "cdninstagram" in t.get("url","")]
                if cand:
                    thumbs = cand
            elif pkey == "twitter":
                cand = [t for t in thumbs
                        if "pbs.twimg.com" in t.get("url","")]
                if cand:
                    # prefer :orig quality
                    best = next((t for t in cand if "orig" in t.get("url","")), cand[0])
                    u = best.get("url","")
                    return u + ("" if "?" in u else "?format=jpg&name=orig")
            elif pkey == "facebook":
                cand = [t for t in thumbs if (t.get("width") or 0) > 200]
                if cand:
                    thumbs = cand

            valid = [t for t in thumbs if t.get("url","").startswith("http")]
            if valid:
                return max(valid,
                    key=lambda t: (t.get("width") or 0) * (t.get("height") or 0)
                ).get("url", "")
        return None

    def _run(self, urls):
        total  = len(urls)
        done   = 0
        prefix = self.prefix_var.get().strip()
        yt_q_lbl = self.yt_q_var.get()
        yt_q_key = next(
            (q[1] for q in self.YT_QUALITIES if q[0] == yt_q_lbl),
            "maxresdefault")

        self.after(0, lambda: self._set_st(
            "Processing {} URL(s)...".format(total), YELLOW))

        for i, url in enumerate(urls):
            if self._cancel:
                break

            pkey  = detect_platform(url)
            pname = PLATFORMS.get(pkey, {}).get("name", "Other")
            self.after(0, lambda n=i+1, t=total, u=url, p=pname:
                self._log("[{}/{}] [{}] {}...".format(n, t, p, u[:55])))

            thumb_url   = None
            thumb_title = "image_{}".format(i + 1)

            try:
                opts = base_ydl_opts(pkey)
                opts["skip_download"] = True
                opts["quiet"]         = True

                # Instagram: mobile UA helps bypass auth walls
                if pkey == "instagram":
                    opts.setdefault("http_headers", {}).update({
                        "User-Agent": (
                            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)"
                            " AppleWebKit/605.1.15 (KHTML, like Gecko)"
                            " Version/15.0 Mobile/15E148 Safari/604.1"
                        )
                    })

                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)

                if not info:
                    raise ValueError("No info returned")

                # Title for filename
                raw_title = (info.get("title") or
                             info.get("description") or
                             thumb_title)
                thumb_title = sanitize(str(raw_title))[:60] or thumb_title

                thumbs = info.get("thumbnails") or []

                # Some platforms expose single image directly in url
                direct = info.get("url", "")
                if direct and any(
                        ext in direct.lower()
                        for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    thumb_url = direct

                if not thumb_url:
                    thumb_url = self._pick_best_thumb(
                        thumbs, info, pkey, yt_q_key)

            except Exception as e:
                err = strip_ansi(str(e))[:100]
                self.after(0, lambda m=err:
                    self._log("  ERROR: {}".format(m)))
                continue

            if not thumb_url:
                self.after(0, lambda:
                    self._log("  ERROR: No image URL found"))
                continue

            # Build filename
            if prefix:
                fname = "{}_{}_{}.jpg".format(prefix, str(i+1).zfill(3), thumb_title)
            else:
                fname = "{}_thumbnail.jpg".format(thumb_title)
            fname    = re.sub(
                r'\.(mp4|mkv|webm|mov|avi|flv|ts|m4v)$', '',
                fname, flags=re.IGNORECASE) + ".jpg"
            out_path = os.path.join(self.save_dir, fname)

            # Download image
            try:
                req  = urllib.request.Request(
                    thumb_url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept":     "image/jpeg,image/png,image/*,*/*",
                        "Referer":    url,
                    })
                resp = urllib.request.urlopen(req, timeout=20)
                data = resp.read()

                ctype = resp.headers.get("Content-Type", "image/jpeg").lower()
                if not any(k in ctype for k in
                           ("image","jpeg","jpg","png","webp")):
                    raise ValueError("Not an image: {}".format(ctype))
                if len(data) < 100:
                    raise ValueError("Response too small")
                if data[:2] not in (b'\xff\xd8', b'\x89P', b'GIF', b'RIFF'):
                    # accept JPEG, PNG, GIF, WEBP magic bytes
                    if data[:4] != b'RIFF':  # WEBP
                        raise ValueError("Not a valid image (bad magic bytes)")

                with open(out_path, "wb") as fp:
                    fp.write(data)

                done  += 1
                kb     = len(data) / 1024
                pct    = (i + 1) / total * 100
                _d     = done
                self.after(0, lambda n=fname, k=kb, p=pct, d=_d: (
                    self._log("  Saved: {}  ({:.1f} KB)".format(n, k)),
                    self.pb.config(value=p),
                    self.cnt_lbl.config(text="{}/{} done".format(d, total))))

            except Exception as e:
                self.after(0, lambda m=str(e):
                    self._log("  SAVE ERROR: {}".format(m[:90])))

        final = done
        self.after(0, lambda: (
            self.pb.config(
                value=100 if not self._cancel else self.pb["value"]),
            self._set_st(
                "{} {}/{} saved".format(
                    "Done:" if not self._cancel else "Cancelled:",
                    final, total),
                GREEN if not self._cancel else MUTED),
            self._log("\n=== {} {}/{} saved to {} ===".format(
                "Done" if not self._cancel else "Cancelled",
                final, total, self.save_dir)),
            self.dl_btn.config(state="normal",
                               text="Download Images / Thumbnails"),
            self.cx_btn.config(state="disabled", bg=DIM, fg=MUTED),
            (messagebox.showinfo("FastTube Pro",
                "{}/{} images downloaded!\nSaved to:\n{}".format(
                    final, total, self.save_dir))
             if final > 0 and not self._cancel else None),
        ))


    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.save_dir)
        if d: self.save_dir=d; self.dir_lbl.config(text=d)

    def _log(self,t):
        self.log.config(state="normal")
        self.log.insert("end",t+"\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def _set_st(self,msg,c=MUTED):
        self.st_lbl.config(text=msg,fg=c)
        self.winfo_toplevel().update_idletasks()

    def _do_cancel(self):
        self._cancel = True
        self._set_st("✖ Cancelling…",MUTED)

    def _start(self):
        ok, msg, _, _ = sub_check_feature("thumbnail")
        if not ok:
            show_limit_popup(self.winfo_toplevel(), msg); return
        urls = []
        u = self.url_var.get().strip()
        if u: urls.append(u)
        bulk = self.bulk_box.get("1.0","end").strip()
        urls += [x.strip() for x in bulk.splitlines() if x.strip()]
        if not urls: messagebox.showwarning("FastTube Pro","URL daalo!"); return

        # ── Duplicate check per URL ───────────────────────────────────────────
        approved = []
        for url in urls:
            is_dup, dup_row = db_check_duplicate(url)
            if is_dup:
                if not ask_duplicate_popup(self.winfo_toplevel(), url, dup_row):
                    continue
            approved.append(url)
        if not approved:
            messagebox.showinfo("FastTube Pro",
                "Saare URLs skip (duplicates). Koi download shuru nahi hua."); return
        urls = approved

        self._cancel = False
        self.log.config(state="normal"); self.log.delete("1.0","end")
        self.log.config(state="disabled")
        self.pb.config(value=0); self.cnt_lbl.config(text="")
        self.dl_btn.config(state="disabled",text="🖼 Downloading…")
        self.cx_btn.config(state="normal",bg=DIM,fg=WHITE)
        threading.Thread(target=self._run,args=(urls,),daemon=True).start()

    def _pick_best_thumb(self, thumbs, info, pkey, yt_q_key):
        """
        Platform-aware thumbnail URL selector.
        Returns best quality image URL or None.
        """
        # ── YouTube: direct CDN URL ─────────────────────────────────────────
        if pkey == "youtube":
            vid_id = info.get("id","")
            if vid_id:
                for qk in [yt_q_key,"sddefault","hqdefault","mqdefault","default"]:
                    test = f"https://img.youtube.com/vi/{vid_id}/{qk}.jpg"
                    try:
                        req  = urllib.request.Request(
                            test, headers={"User-Agent":"Mozilla/5.0"})
                        resp = urllib.request.urlopen(req, timeout=8)
                        if int(resp.headers.get("Content-Length",99999)) > 2000:
                            return test
                    except Exception:
                        continue

        # ── Instagram: prefer scontent CDN, highest res ────────────────────
        if pkey == "instagram":
            ig_thumbs = [t for t in thumbs
                         if "scontent" in t.get("url","")
                         or "cdninstagram" in t.get("url","")]
            if ig_thumbs:
                thumbs = ig_thumbs

        # ── Twitter/X: pick largest available image ────────────────────────
        if pkey == "twitter":
            tw_thumbs = [t for t in thumbs
                         if "pbs.twimg.com" in t.get("url","")]
            if tw_thumbs:
                # Try :orig quality first
                best_orig = next((t for t in tw_thumbs
                                  if "orig" in t.get("url","")), None)
                if best_orig:
                    return best_orig["url"] + "?format=jpg&name=orig"
                thumbs = tw_thumbs

        # ── WhatsApp: status/image thumbnails via yt-dlp metadata ─────────
        # ── Snapchat: spotlight thumbnails ────────────────────────────────
        # ── Facebook: prefer largest, skip low-res placeholders ───────────
        if pkey == "facebook":
            fb_thumbs = [t for t in thumbs
                         if (t.get("width",0) or 0) > 200]
            if fb_thumbs:
                thumbs = fb_thumbs

        # ── Generic: pick highest resolution ──────────────────────────────
        if thumbs:
            valid = [t for t in thumbs if t.get("url","").startswith("http")]
            if valid:
                return max(
                    valid,
                    key=lambda t: (t.get("width",0) or 0)*(t.get("height",0) or 0)
                ).get("url","")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN APPLICATION WINDOW
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  PROFILE DIALOG
# ══════════════════════════════════════════════════════════════════════════════
class ProfileDialog(tk.Toplevel):
    """User profile popup — shows account info, plan, usage, logout."""

    def __init__(self, master, on_logout=None, on_plan_change=None):
        super().__init__(master)
        self._on_logout      = on_logout
        self._on_plan_change = on_plan_change
        self.title("My Profile — FastTube Pro")
        self.geometry("420x560")
        self.resizable(False, False)
        self.configure(bg=BG)
        self.grab_set()
        self._build()
        self._center()

    def _center(self):
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _row(self, parent, label, value, val_color=None):
        f = tk.Frame(parent, bg=BG_CARD)
        f.pack(fill="x", pady=3)
        tk.Label(f, text=label,
                 font=("Helvetica", 8, "bold"),
                 fg=MUTED, bg=BG_CARD,
                 width=16, anchor="w").pack(side="left", padx=(0,6))
        tk.Label(f, text=str(value),
                 font=("Helvetica", 9),
                 fg=val_color or WHITE,
                 bg=BG_CARD, anchor="w").pack(side="left", fill="x", expand=True)

    def _build(self):
        _refresh_sub()
        plan    = _S.get("plan", "FREE")
        name    = _S.get("name", "")
        email   = _S.get("email", "")
        expiry  = _S.get("expiry", "") or ""
        daily   = _S.get("daily", 0)
        weekly  = _S.get("weekly", 0)
        pl_cols = {"FREE":MUTED,"DAILY":BLUE,"MONTHLY":GREEN,"YEARLY":YELLOW}
        pl_col  = pl_cols.get(plan, MUTED)
        pi      = PLANS.get(plan, PLANS["FREE"])

        # ── Scrollable canvas (top-to-bottom scroll) ──────────────────────────
        canvas = tk.Canvas(self, bg=BG, highlightthickness=0)
        vsb    = ttk.Scrollbar(self, orient="vertical",
                               command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=BG)
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_resize(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def _on_canvas_resize(e):
            canvas.itemconfig(win_id, width=e.width)
        inner.bind("<Configure>", _on_inner_resize)
        canvas.bind("<Configure>", _on_canvas_resize)

        # Mouse wheel scroll
        def _on_wheel(e):
            canvas.yview_scroll(int(-1*(e.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_wheel)

        # ── Avatar section ────────────────────────────────────────────────────
        av_f = tk.Frame(inner, bg=BG, pady=20)
        av_f.pack(fill="x")

        initials = "".join(p[0].upper() for p in name.split()[:2]) or "?"
        tk.Label(av_f, text=initials,
                 font=("Helvetica", 22, "bold"),
                 fg=WHITE, bg=pl_col,
                 width=3, height=1).pack()

        tk.Label(av_f, text=name,
                 font=("Helvetica", 14, "bold"),
                 fg=WHITE, bg=BG).pack(pady=(8, 0))
        tk.Label(av_f, text=email,
                 font=("Helvetica", 9),
                 fg=MUTED, bg=BG).pack(pady=(2, 0))
        tk.Label(av_f,
                 text=f"  {plan} PLAN  ",
                 font=("Helvetica", 9, "bold"),
                 fg=BG if pl_col in (YELLOW, GREEN) else WHITE,
                 bg=pl_col, padx=10, pady=4).pack(pady=(10, 0))

        tk.Frame(inner, bg=BORDER, height=1).pack(fill="x",
                 padx=16, pady=(14, 0))

        # ── Account Details card ──────────────────────────────────────────────
        sec1 = tk.Frame(inner, bg=BG_CARD, padx=16, pady=14)
        sec1.pack(fill="x", padx=16, pady=(12, 0))
        tk.Label(sec1, text="📋  Account Details",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w", pady=(0, 10))

        self._row(sec1, "Full Name",   name)
        self._row(sec1, "Email",       email)
        self._row(sec1, "Plan",        pi["name"], pl_col)
        self._row(sec1, "Max Quality", f"{pi['max_res']}p")
        self._row(sec1, "Batch DL",
                  "✅ Enabled" if pi["batch"] else "❌ Monthly+ only",
                  GREEN if pi["batch"] else MUTED)
        self._row(sec1, "Metadata",
                  "✅ Enabled" if pi["meta"] else "❌ Monthly+ only",
                  GREEN if pi["meta"] else MUTED)
        self._row(sec1, "Compare",
                  "✅ Enabled" if pi["compare"] else "❌ Yearly only",
                  GREEN if pi["compare"] else MUTED)

        # ── Usage & Validity card ─────────────────────────────────────────────
        sec2 = tk.Frame(inner, bg=BG_CARD, padx=16, pady=14)
        sec2.pack(fill="x", padx=16, pady=(10, 0))
        tk.Label(sec2, text="📊  Usage & Validity",
                 font=("Helvetica", 9, "bold"),
                 fg=WHITE, bg=BG_CARD).pack(anchor="w", pady=(0, 10))

        if plan == "FREE":
            used      = weekly
            lim       = pi["dl_week"]
            remaining = max(0, lim - used)
            self._row(sec2, "Used This Week",
                      f"{used} / {lim}",
                      YELLOW if used >= lim else WHITE)
            self._row(sec2, "Remaining",
                      f"{remaining} video{'s' if remaining != 1 else ''}",
                      GREEN if remaining > 0 else ACCENT)
            self._row(sec2, "Resets Every", "7 days from first download",
                      MUTED)
            self._row(sec2, "Plan Expiry", "Never — Free forever", MUTED)

        elif plan == "DAILY":
            used      = daily
            lim       = pi["dl_day"]
            remaining = max(0, lim - used)
            self._row(sec2, "Used This Week",
                      f"{used} / {lim}",
                      YELLOW if used >= lim else WHITE)
            self._row(sec2, "Remaining",
                      f"{remaining} video{'s' if remaining != 1 else ''}",
                      GREEN if remaining > 0 else ACCENT)
            self._row(sec2, "Resets Every", "7 days", MUTED)
            exp_show = expiry[:10] if expiry else "N/A"
            self._row(sec2, "Valid Until", exp_show,
                      GREEN if expiry else MUTED)

        else:
            self._row(sec2, "Downloads", "Unlimited  ∞", GREEN)
            exp_show = expiry[:10] if expiry else "N/A"
            self._row(sec2, "Valid Until", exp_show,
                      GREEN if expiry else MUTED)

        # ── Action buttons ────────────────────────────────────────────────────
        btn_f = tk.Frame(inner, bg=BG, padx=16)
        btn_f.pack(fill="x", pady=14)

        tk.Button(btn_f, text="⚡  Upgrade Plan",
                  font=("Helvetica", 10, "bold"),
                  bg=ACCENT, fg=WHITE, relief="flat", bd=0,
                  pady=10, cursor="hand2",
                  command=self._upgrade).pack(fill="x", pady=(0, 8))

        tk.Button(btn_f, text="🔑  Change Password",
                  font=("Helvetica", 9),
                  bg=BG_CARD, fg=MUTED, relief="flat", bd=0,
                  pady=9, cursor="hand2",
                  command=self._change_pw).pack(fill="x", pady=(0, 8))

        tk.Button(btn_f, text="↩  Logout",
                  font=("Helvetica", 9, "bold"),
                  bg="#2a0a0a", fg=ACCENT, relief="flat", bd=0,
                  pady=8, cursor="hand2",
                  command=self._logout).pack(fill="x")

    def _upgrade(self):
        self.destroy()
        UpgradeDialog(self.master, on_done=self._on_plan_change)

    def _change_pw(self):
        """Change password dialog."""
        win = tk.Toplevel(self)
        win.title("Change Password")
        win.geometry("380x320")
        win.resizable(False, False)
        win.configure(bg=BG)
        win.grab_set()
        win.update_idletasks()
        sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
        win.geometry(f"380x320+{(sw-380)//2}+{(sh-320)//2}")

        tk.Label(win, text="🔑  Change Password",
                 font=("Helvetica", 13, "bold"),
                 fg=WHITE, bg=BG).pack(pady=(22, 14))

        frm = tk.Frame(win, bg=BG, padx=30)
        frm.pack(fill="x")

        def _entry(lbl, show=""):
            tk.Label(frm, text=lbl,
                     font=("Helvetica", 8, "bold"),
                     fg=MUTED, bg=BG).pack(anchor="w")
            v = tk.StringVar()
            tk.Entry(frm, textvariable=v, show=show,
                     font=("Helvetica", 10), bg=BG_INPUT, fg=WHITE,
                     insertbackground=WHITE, relief="flat", bd=0
                     ).pack(fill="x", ipady=7, pady=(2, 10))
            return v

        cur_v  = _entry("Current Password", "●")
        new_v  = _entry("New Password  (min 6 chars)", "●")
        cnew_v = _entry("Confirm New Password", "●")

        st = tk.Label(frm, text="", font=("Helvetica", 8),
                       fg=ACCENT, bg=BG)
        st.pack()

        def _save():
            cur  = cur_v.get()
            npw  = new_v.get()
            cpw  = cnew_v.get()
            conn = _auth_conn()
            row  = conn.execute(
                "SELECT id FROM users WHERE id=? AND password_hash=?",
                (_S["uid"], _hash_pw(cur))).fetchone()
            if not row:
                st.config(text="Current password galat hai.", fg=ACCENT)
                conn.close(); return
            if len(npw) < 6:
                st.config(text="Min 6 characters.", fg=ACCENT)
                conn.close(); return
            if npw != cpw:
                st.config(text="Passwords match nahi.", fg=ACCENT)
                conn.close(); return
            conn.execute("UPDATE users SET password_hash=? WHERE id=?",
                         (_hash_pw(npw), _S["uid"]))
            conn.commit(); conn.close()
            st.config(text="✅ Password changed!", fg=GREEN)
            win.after(1500, win.destroy)

        tk.Button(frm, text="Save Password",
                  font=("Helvetica", 10, "bold"),
                  bg=GREEN, fg=BG, relief="flat", bd=0,
                  pady=9, cursor="hand2",
                  command=_save).pack(fill="x", pady=(6, 0))

    def _logout(self):
        if messagebox.askyesno("Logout",
            "Logout karna chahte ho?\nNext time login karna hoga.",
            parent=self):
            self.destroy()
            if self._on_logout:
                self._on_logout()


class FastTubeApp:

    TABS = [
        ("📦 Batch",     BatchTab),
        ("⬇ Single",    SingleTab),
        ("🎵 Audio",     AudioTab),
        ("🔄 Convert",   ConverterTab),
        ("📄 Subtitles", SubtitleTab),
        ("ℹ Metadata",   MetadataTab),
        ("🖼 Thumbnail", ThumbnailTab),
    ]

    def __init__(self, root):
        self.root = root
        root.title("FastTube Pro — Universal Downloader")
        root.geometry("920x760")
        root.minsize(820, 560)
        root.resizable(True, True)
        root.configure(bg=BG)
        self._set_icon()
        self._build()

    def _set_icon(self):
        try:
            base = (os.path.dirname(sys.executable)
                    if getattr(sys,"frozen",False)
                    else os.path.dirname(os.path.abspath(__file__)))
            ico = os.path.join(base,"assets","icon.ico")
            if os.path.exists(ico):
                self.root.iconbitmap(ico)
        except Exception:
            pass

    def _build(self):
        # ── Header ──────────────────────────────────────────────────────────
        # IMPORTANT: In Tkinter pack(), side="right" widgets MUST be packed
        # BEFORE side="left" widgets, otherwise left widgets push right ones
        # off screen (that's why title was getting cut off).
        hdr = tk.Frame(self.root, bg=BG, pady=6)
        hdr.pack(fill="x")

        # ── RIGHT SIDE — pack first so they claim space before left expands ──
        right = tk.Frame(hdr, bg=BG)
        right.pack(side="right", padx=(0, 12))

        # FFmpeg badge
        ff_ok  = ffmpeg_ok()
        ff_bg  = "#0a2a0a" if ff_ok else "#3a1a00"
        ff_fg  = GREEN     if ff_ok else YELLOW
        ff_txt = "✅ FFmpeg" if ff_ok else "⚠ FFmpeg"
        ffbadge = tk.Frame(right, bg=ff_bg, padx=6, pady=3)
        ffbadge.pack(side="right", padx=(6, 0))
        tk.Label(ffbadge, text=ff_txt,
                 font=("Helvetica", 7), fg=ff_fg, bg=ff_bg).pack()

        # Upgrade button
        Btn(right, "⚡ Upgrade",
            lambda: UpgradeDialog(self.root,
                                  on_done=self._refresh_plan_badge),
            bg=ACCENT, fg=WHITE, fs=8, py=4, px=10
            ).pack(side="right", padx=(4, 0))

        # Set Cookies
        Btn(right, "🍪 Cookies", self._set_cookies,
            bg=BG_CARD, fg=MUTED, fs=8, py=4, px=8
            ).pack(side="right", padx=(4, 0))

        # Profile badge — clickable → ProfileDialog
        plan    = _S.get("plan", "FREE")
        pl_cols = {"FREE": MUTED, "DAILY": BLUE,
                   "MONTHLY": GREEN, "YEARLY": YELLOW}
        pl_col  = pl_cols.get(plan, MUTED)
        uname   = (_S.get("name") or "User").split()[0]
        self._plan_badge_lbl = tk.Label(
            right,
            text=f"👤 {uname}  [{plan}]",
            font=("Helvetica", 8, "bold"),
            fg=pl_col, bg=BG_CARD,
            padx=10, pady=5, cursor="hand2")
        self._plan_badge_lbl.pack(side="right", padx=(4, 0))
        self._plan_badge_lbl.bind(
            "<Button-1>", lambda e: self._open_profile())

        # ── LEFT SIDE — logo + platform badges ──────────────────────────────
        tk.Label(hdr, text="⚡ FastTube Pro",
                 font=("Helvetica", 18, "bold"),
                 fg=ACCENT, bg=BG).pack(side="left", padx=(14, 8))

        for key, info in PLATFORMS.items():
            tk.Label(hdr,
                     text=f"{info['icon']}{info['name']}",
                     font=("Helvetica", 7, "bold"),
                     fg=info["color"], bg=BG, padx=3
                     ).pack(side="left")

        tk.Frame(self.root, bg=BORDER, height=1).pack(fill="x")

        # ── Notebook Tabs ────────────────────────────────────────────────────
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("FT.TNotebook",
                    background=BG, bordercolor=BG, tabmargins=[2,2,2,0])
        s.configure("FT.TNotebook.Tab",
                    background="#111111", foreground=MUTED,
                    font=("Helvetica",9,"bold"), padding=[14,7],
                    bordercolor=BG)
        s.map("FT.TNotebook.Tab",
              background=[("selected", BG_CARD)],
              foreground=[("selected", WHITE)])

        nb = ttk.Notebook(self.root, style="FT.TNotebook")
        nb.pack(fill="both", expand=True)

        for name, TabClass in self.TABS:
            frame = TabClass(nb)
            nb.add(frame, text=name)

        # ── Footer ───────────────────────────────────────────────────────────
        tk.Frame(self.root, bg=BORDER, height=1).pack(fill="x")
        foot = tk.Frame(self.root, bg=BG, pady=4)
        foot.pack(fill="x")
        tk.Label(foot,
                 text="⚠  Personal & educational use only. Respect copyright laws.",
                 font=("Helvetica",8), fg=DIM, bg=BG
                 ).pack(side="left", padx=16)
        tk.Label(foot,
                 text=f"Save dir: {get_save_dir()}",
                 font=("Helvetica",7), fg=DIM, bg=BG
                 ).pack(side="right", padx=16)

    def _refresh_plan_badge(self):
        """Refresh plan badge in header after plan change."""
        _refresh_sub()
        plan    = _S.get("plan","FREE")
        pl_cols = {"FREE":MUTED,"DAILY":BLUE,"MONTHLY":GREEN,"YEARLY":YELLOW}
        pl_col  = pl_cols.get(plan, MUTED)
        first   = _S.get("name","").split()[0] if _S.get("name") else "User"
        if hasattr(self,"_plan_badge_lbl"):
            self._plan_badge_lbl.config(
                text=f"👤 {first}  [{plan}]", fg=pl_col)

    def _open_profile(self):
        ProfileDialog(self.root, on_logout=self._do_logout,
                      on_plan_change=self._refresh_plan_badge)

    def _do_logout(self):
        _clear_sess()
        self.root.destroy()
        import subprocess
        subprocess.Popen([sys.executable] + sys.argv)

    def _logout(self):
        if messagebox.askyesno("Logout",
            "Logout karna chahte ho?",
            parent=self.root):
            self._do_logout()

    def _set_cookies(self):
        """Cookie setup dialog — file import ya browser se extract."""
        win = tk.Toplevel(self.root)
        win.title("🍪 Cookie Setup")
        win.geometry("480x400")
        win.resizable(False, False)
        win.configure(bg=BG)
        win.grab_set()

        tk.Label(win, text="🍪 Cookie Setup",
                 font=("Helvetica",13,"bold"), fg=WHITE, bg=BG).pack(pady=(18,4))
        tk.Label(win,
                 text="Facebook / Instagram login ke liye cookies zaroori hain.",
                 font=("Helvetica",8), fg=MUTED, bg=BG).pack()

        # ── Option 1: Browser auto-import ─────────────────────────────────
        fr1 = tk.Frame(win, bg=BG_CARD, padx=14, pady=12)
        fr1.pack(fill="x", padx=16, pady=(14,6))
        tk.Label(fr1, text="Option 1 — Browser se Auto-Import",
                 font=("Helvetica",9,"bold"), fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(fr1,
                 text="Apne browser mein Facebook/Instagram login karein,\n"
                      "phir browser choose karke Import dabayein.",
                 font=("Helvetica",8), fg=MUTED, bg=BG_CARD, justify="left").pack(anchor="w", pady=(4,8))

        br_row = tk.Frame(fr1, bg=BG_CARD); br_row.pack(fill="x")
        br_var = tk.StringVar(value="chrome")
        for bname, bval in [("Chrome","chrome"),("Firefox","firefox"),
                             ("Edge","edge"),("Brave","brave"),("Opera","opera")]:
            tk.Radiobutton(br_row, text=bname, variable=br_var, value=bval,
                           font=("Helvetica",8), fg=WHITE, bg=BG_CARD,
                           selectcolor=BG_INPUT, activebackground=BG_CARD,
                           activeforeground=WHITE).pack(side="left", padx=6)

        st_lbl = tk.Label(fr1, text="", font=("Helvetica",8), fg=MUTED, bg=BG_CARD)
        st_lbl.pack(anchor="w", pady=(6,0))

        def _import_from_browser():
            browser = br_var.get()
            st_lbl.config(text="⏳ Importing from {}…".format(browser), fg=YELLOW)
            win.update_idletasks()
            try:
                base = (os.path.dirname(sys.executable)
                        if getattr(sys,"frozen",False)
                        else os.path.dirname(os.path.abspath(__file__)))
                dest = os.path.join(base, "cookies.txt")
                # Use yt-dlp subprocess to extract cookies from browser
                result = subprocess.run(
                    [sys.executable, "-m", "yt_dlp",
                     "--cookies-from-browser", browser,
                     "--cookies", dest,
                     "--skip-download",
                     "https://www.facebook.com"],
                    capture_output=True, text=True, timeout=45)
                if os.path.exists(dest) and os.path.getsize(dest) > 100:
                    st_lbl.config(
                        text="✅ Cookies imported from {}! Restart downloads.".format(browser),
                        fg=GREEN)
                else:
                    out = (result.stderr or result.stdout or "")[:100]
                    st_lbl.config(text="❌ Failed: " + out, fg=ACCENT)
            except Exception as e:
                st_lbl.config(text="❌ " + str(e)[:90], fg=ACCENT)

        Btn(fr1, "🌐 Import from Browser", _import_from_browser,
            bg=BLUE, fg=WHITE, fs=9, py=7).pack(fill="x", pady=(8,0))

        # ── Option 2: Manual file ──────────────────────────────────────────
        fr2 = tk.Frame(win, bg=BG_CARD, padx=14, pady=12)
        fr2.pack(fill="x", padx=16, pady=(0,6))
        tk.Label(fr2, text="Option 2 — cookies.txt File Import",
                 font=("Helvetica",9,"bold"), fg=WHITE, bg=BG_CARD).pack(anchor="w")
        tk.Label(fr2,
                 text="Browser extension: 'Get cookies.txt LOCALLY' se export karein",
                 font=("Helvetica",8), fg=MUTED, bg=BG_CARD).pack(anchor="w", pady=(2,6))

        def _import_file():
            f = filedialog.askopenfilename(
                title="Select cookies.txt",
                filetypes=[("Text","*.txt"),("All","*.*")])
            if f:
                base = (os.path.dirname(sys.executable)
                        if getattr(sys,"frozen",False)
                        else os.path.dirname(os.path.abspath(__file__)))
                dest = os.path.join(base, "cookies.txt")
                shutil.copy(f, dest)
                messagebox.showinfo("FastTube Pro",
                    "✅ Cookies loaded!\n\n"
                    "Facebook/Instagram: Private content unlock\n"
                    "YouTube: 1080p/4K bot-bypass active", parent=win)
                win.destroy()

        Btn(fr2, "📂 Select cookies.txt File", _import_file,
            bg=BG_INPUT, fg=MUTED, fs=9, py=7).pack(fill="x")

        # ── Current status ─────────────────────────────────────────────────
        ck     = get_cookies()
        ck_msg = "✅ cookies.txt active" if ck else "⚠ No cookies — Facebook/Instagram may fail"
        ck_col = GREEN if ck else YELLOW
        tk.Label(win, text=ck_msg,
                 font=("Helvetica",7,"bold"), fg=ck_col, bg=BG).pack(pady=(4,0))

        tk.Button(win, text="Close", font=("Helvetica",9),
                  bg=BG_CARD, fg=MUTED, relief="flat", bd=0,
                  padx=20, pady=6, cursor="hand2",
                  command=win.destroy).pack(pady=(10,0))


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()  # Hide main window until auth done

    def _on_auth_ok():
        root.deiconify()       # Show main window
        FastTubeApp(root)

    if _load_sess():
        # Auto-login: session found → go directly to app
        _on_auth_ok()
    else:
        # No session → show Login/Signup window
        AuthWindow(root, on_success=_on_auth_ok)

    root.mainloop()
