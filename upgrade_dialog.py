"""
subscription_service.py — FastTube Pro
=======================================
Standalone subscription service.
Import this in fasttube.py or any other module.

Usage:
    from subscription_service import can_download, record_download, activate_plan
"""

import sqlite3
import os
import re
from datetime import datetime, timedelta

# ── DB path — same folder as FastTube save dir ───────────────────────────────
_SAVE_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "FastTube")
_AUTH_DB  = os.path.join(_SAVE_DIR, "fasttube_auth.db")

# ── Plan definitions ──────────────────────────────────────────────────────────
PLANS = {
    "FREE": {
        "name":     "Free",
        "price":    0,
        "dl_week":  1,      # max downloads per week
        "dl_day":   0,      # max downloads per day (0 = not applicable)
        "max_res":  720,    # max resolution in pixels
        "batch":    False,  # batch download allowed
        "metadata": False,  # Metadata Studio allowed
        "compare":  False,  # Compare Export allowed
    },
    "DAILY": {
        "name":     "Daily",
        "price":    51,
        "dl_week":  0,
        "dl_day":   3,
        "max_res":  1080,
        "batch":    False,
        "metadata": False,
        "compare":  False,
    },
    "MONTHLY": {
        "name":     "Monthly",
        "price":    199,
        "dl_week":  0,
        "dl_day":   -1,     # -1 = unlimited
        "max_res":  2160,
        "batch":    True,
        "metadata": True,
        "compare":  False,
    },
    "YEARLY": {
        "name":     "Yearly",
        "price":    899,
        "dl_week":  0,
        "dl_day":   -1,
        "max_res":  2160,
        "batch":    True,
        "metadata": True,
        "compare":  True,
    },
}


def _conn():
    os.makedirs(_SAVE_DIR, exist_ok=True)
    return sqlite3.connect(_AUTH_DB)


def _get_active_sub(user_id):
    """Fetch active subscription row for user. Auto-expires if needed."""
    conn = _conn()
    row  = conn.execute(
        "SELECT id,plan_type,expiry_date,daily_count,weekly_count,"
        "last_reset,status "
        "FROM user_subscription "
        "WHERE user_id=? ORDER BY id DESC LIMIT 1",
        (user_id,)
    ).fetchone()

    if not row:
        conn.close()
        return {"plan": "FREE", "daily": 0, "weekly": 0, "expiry": None}

    sub_id, plan, expiry, dc, wc, lr, status = row
    now = datetime.now()

    # ── Auto expiry ───────────────────────────────────────────────────────────
    if expiry and status == "ACTIVE":
        try:
            if now > datetime.fromisoformat(expiry):
                conn.execute(
                    "UPDATE user_subscription "
                    "SET status='EXPIRED', plan_type='FREE' "
                    "WHERE id=?", (sub_id,))
                conn.commit()
                plan   = "FREE"
                expiry = None
                dc = wc = 0
        except Exception:
            pass

    # ── Daily reset (DAILY plan) ──────────────────────────────────────────────
    if plan == "DAILY" and lr:
        try:
            if now - datetime.fromisoformat(lr) > timedelta(hours=24):
                conn.execute(
                    "UPDATE user_subscription "
                    "SET daily_count=0, last_reset=? "
                    "WHERE id=?", (now.isoformat(), sub_id))
                conn.commit()
                dc = 0
        except Exception:
            pass

    # ── Weekly reset (FREE plan) ──────────────────────────────────────────────
    if plan == "FREE" and lr:
        try:
            if now - datetime.fromisoformat(lr) > timedelta(days=7):
                conn.execute(
                    "UPDATE user_subscription "
                    "SET weekly_count=0, last_reset=? "
                    "WHERE id=?", (now.isoformat(), sub_id))
                conn.commit()
                wc = 0
        except Exception:
            pass

    conn.close()
    return {
        "plan":   plan,
        "daily":  dc  or 0,
        "weekly": wc  or 0,
        "expiry": expiry,
        "sub_id": sub_id,
    }


def can_download(user_id, quality="720p"):
    """
    Check if user can download.

    Args:
        user_id : int  — user's DB id
        quality : str  — e.g. "720p", "1080p", "2160p", "best"

    Returns:
        (allowed: bool, reason: str)
        allowed=True  → download can proceed
        allowed=False → reason explains why blocked
    """
    sub  = _get_active_sub(user_id)
    plan = sub["plan"]
    pi   = PLANS.get(plan, PLANS["FREE"])

    # ── Quality gate ─────────────────────────────────────────────────────────
    if quality and quality != "best" and quality != "audio_mp3":
        try:
            h = int(re.search(r'(\d+)', quality).group(1))
            if h > pi["max_res"]:
                needed = "Monthly or Yearly" if h <= 2160 else "Yearly"
                return False, (
                    f"{quality} quality requires {needed} plan.\n"
                    "Upgrade to continue."
                )
        except Exception:
            pass

    # ── Count gate ────────────────────────────────────────────────────────────
    if plan == "FREE":
        if sub["weekly"] >= pi["dl_week"]:
            return False, (
                f"Free plan: {pi['dl_week']} video/week limit reached.\n"
                "Upgrade to continue downloading."
            )

    elif plan == "DAILY":
        if sub["daily"] >= pi["dl_day"]:
            return False, (
                f"Daily plan: {pi['dl_day']} videos/day limit reached.\n"
                "Upgrade or wait 24 hours."
            )

    # MONTHLY / YEARLY → unlimited
    return True, ""


def record_download(user_id):
    """
    Increment download counter after successful download.
    Call this AFTER download completes.
    """
    sub  = _get_active_sub(user_id)
    plan = sub["plan"]
    now  = datetime.now().isoformat()

    if plan not in ("FREE", "DAILY"):
        return  # unlimited — no counting needed

    conn = _conn()
    sub_id = sub.get("sub_id")
    if not sub_id:
        conn.close()
        return

    if plan == "FREE":
        conn.execute(
            "UPDATE user_subscription "
            "SET weekly_count=weekly_count+1, "
            "last_reset=COALESCE(last_reset,?) "
            "WHERE id=?",
            (now, sub_id))
    elif plan == "DAILY":
        conn.execute(
            "UPDATE user_subscription "
            "SET daily_count=daily_count+1, "
            "last_reset=COALESCE(last_reset,?) "
            "WHERE id=?",
            (now, sub_id))

    conn.commit()
    conn.close()


def feature_allowed(user_id, feature):
    """
    Check if a specific feature is allowed for user's plan.

    Args:
        user_id : int
        feature : str — "batch" | "4k" | "metadata" | "compare"

    Returns:
        (allowed: bool, reason: str)
    """
    sub  = _get_active_sub(user_id)
    plan = sub["plan"]
    pi   = PLANS.get(plan, PLANS["FREE"])

    rules = {
        "batch": (
            pi["batch"],
            "Batch download requires Monthly or Yearly plan."
        ),
        "4k": (
            pi["max_res"] >= 2160,
            "4K quality requires Monthly or Yearly plan."
        ),
        "metadata": (
            pi["metadata"],
            "Metadata Studio requires Monthly or Yearly plan."
        ),
        "compare": (
            pi["compare"],
            "Compare Export requires Yearly plan."
        ),
    }

    ok, msg = rules.get(feature, (True, ""))
    return ok, ("" if ok else msg)


def activate_plan(user_id, plan_key):
    """
    Activate a plan for user. Deletes old subscription.

    Args:
        user_id  : int
        plan_key : str — "FREE" | "DAILY" | "MONTHLY" | "YEARLY"

    Returns:
        (success: bool, expiry_str: str)
    """
    days_map = {"FREE": None, "DAILY": 1, "MONTHLY": 30, "YEARLY": 365}
    d        = days_map.get(plan_key)
    now      = datetime.now()
    expiry   = (now + timedelta(days=d)).isoformat() if d else None

    conn = _conn()
    try:
        conn.execute(
            "DELETE FROM user_subscription WHERE user_id=?", (user_id,))
        conn.execute(
            "INSERT INTO user_subscription"
            "(user_id, plan_type, start_date, expiry_date, "
            "daily_count, weekly_count, last_reset, status) "
            "VALUES (?,?,?,?,0,0,?,?)",
            (user_id, plan_key, now.isoformat(), expiry,
             now.isoformat(), "ACTIVE"))
        conn.commit()
        conn.close()
        return True, expiry or ""
    except Exception as e:
        conn.close()
        return False, str(e)


def get_plan_info(user_id):
    """
    Returns dict with full plan info for display.

    Returns:
        {
            "plan":   "FREE" | "DAILY" | "MONTHLY" | "YEARLY",
            "name":   "Free" | "Daily" | ...
            "expiry": "2025-03-15T12:00:00" | None,
            "daily":  int,
            "weekly": int,
        }
    """
    sub  = _get_active_sub(user_id)
    plan = sub["plan"]
    pi   = PLANS.get(plan, PLANS["FREE"])
    return {
        "plan":   plan,
        "name":   pi["name"],
        "expiry": sub.get("expiry"),
        "daily":  sub.get("daily", 0),
        "weekly": sub.get("weekly", 0),
    }


# ── Schema creation (call once on app start) ──────────────────────────────────
def ensure_tables():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TEXT DEFAULT (datetime('now')),
            last_login    TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_subscription (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            plan_type    TEXT    DEFAULT 'FREE',
            start_date   TEXT,
            expiry_date  TEXT,
            daily_count  INTEGER DEFAULT 0,
            weekly_count INTEGER DEFAULT 0,
            last_reset   TEXT,
            status       TEXT    DEFAULT 'ACTIVE',
            FOREIGN KEY(user_id) REFERENCES users(id)
        )""")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    ensure_tables()
    print("subscription_service.py — OK")
    print("Tables created in:", _AUTH_DB)
