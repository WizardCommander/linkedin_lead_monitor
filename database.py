import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

DB_NAME = "pr_leads.db"


def init_database():
    """Initialize database with required tables"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            post_id TEXT UNIQUE NOT NULL,
            author_name TEXT,
            author_handle TEXT,
            author_title TEXT,
            company_name TEXT,
            post_content TEXT,
            post_url TEXT,
            budget_mention TEXT,
            created_at TEXT,
            scraped_at TEXT NOT NULL,
            matched_keywords TEXT,
            matched_roles TEXT,
            matched_categories TEXT,
            dismissed INTEGER DEFAULT 0,
            raw_data TEXT
        )
    """
    )

    # Migration: Add new columns if they don't exist
    try:
        cursor.execute("ALTER TABLE leads ADD COLUMN matched_keywords TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE leads ADD COLUMN matched_roles TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE leads ADD COLUMN matched_categories TEXT")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE leads ADD COLUMN dismissed INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_ids (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            activity_id TEXT NOT NULL,
            original_url TEXT,
            discovered_at TEXT NOT NULL,
            scraped BOOLEAN DEFAULT 0,
            UNIQUE(platform, activity_id)
        )
    """
    )

    # Migration: Add original_url column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE activity_ids ADD COLUMN original_url TEXT")
    except sqlite3.OperationalError:
        pass

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_platform ON leads(platform)
    """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scraped_at ON leads(scraped_at)
    """
    )

    conn.commit()
    conn.close()


def save_lead(platform: str, post_id: str, data: Dict) -> bool:
    """Save a single lead to database"""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO leads
                (platform, post_id, author_name, author_handle, author_title,
                 company_name, post_content, post_url, budget_mention,
                 created_at, scraped_at, matched_keywords, matched_roles,
                 matched_categories, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    platform,
                    post_id,
                    data.get("author_name"),
                    data.get("author_handle"),
                    data.get("author_title"),
                    data.get("company_name"),
                    data.get("post_content"),
                    data.get("post_url"),
                    data.get("budget_mention"),
                    data.get("created_at"),
                    datetime.now().isoformat(),
                    data.get("matched_keywords"),
                    data.get("matched_roles"),
                    data.get("matched_categories"),
                    data.get("raw_data"),
                ),
            )
            return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


def save_activity_id(platform: str, activity_id: str, original_url: str = None) -> bool:
    """Save an activity ID to track discovered posts

    Args:
        platform: Platform name (e.g., "linkedin")
        activity_id: Unique activity identifier
        original_url: Original URL of the post (optional but recommended)

    Returns:
        True if new activity ID was inserted, False if already exists
    """
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO activity_ids (platform, activity_id, original_url, discovered_at)
                VALUES (?, ?, ?, ?)
            """,
                (platform, activity_id, original_url, datetime.now().isoformat()),
            )
            return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


def mark_activity_scraped(platform: str, activity_id: str):
    """Mark an activity ID as scraped"""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE activity_ids
            SET scraped = 1
            WHERE platform = ? AND activity_id = ?
        """,
            (platform, activity_id),
        )


def get_unscraped_activity_ids(platform: str) -> List[tuple]:
    """Get activity IDs that haven't been scraped yet

    Args:
        platform: Platform name (e.g., "linkedin")

    Returns:
        List of (activity_id, original_url) tuples.
        If original_url is None, caller should reconstruct the URL.
    """
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT activity_id, original_url FROM activity_ids
            WHERE platform = ? AND scraped = 0
        """,
            (platform,),
        )
        return cursor.fetchall()


def get_lead_count(platform: Optional[str] = None) -> int:
    """Get total number of leads, optionally filtered by platform"""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        if platform:
            cursor.execute("SELECT COUNT(*) FROM leads WHERE platform = ?", (platform,))
        else:
            cursor.execute("SELECT COUNT(*) FROM leads")

        return cursor.fetchone()[0]


def get_recent_leads(platform: Optional[str] = None, limit: int = 10) -> List[Dict]:
    """Get most recent leads"""
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if platform:
            cursor.execute(
                """
                SELECT * FROM leads
                WHERE platform = ?
                ORDER BY scraped_at DESC
                LIMIT ?
            """,
                (platform, limit),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM leads
                ORDER BY scraped_at DESC
                LIMIT ?
            """,
                (limit,),
            )

        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_leads_filtered(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    role: Optional[str] = None,
    keyword: Optional[str] = None,
    search_text: Optional[str] = None,
    include_dismissed: bool = False,
    limit: Optional[int] = None,
) -> List[Dict]:
    """Get leads with advanced filtering"""
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = "SELECT * FROM leads WHERE 1=1"
        params = []

        if platform:
            query += " AND platform = ?"
            params.append(platform)

        if not include_dismissed:
            query += " AND (dismissed = 0 OR dismissed IS NULL)"

        if category:
            query += " AND matched_categories LIKE ?"
            params.append(f'%"{category}"%')

        if role:
            query += " AND matched_roles LIKE ?"
            params.append(f'%"{role}"%')

        if keyword:
            query += " AND matched_keywords LIKE ?"
            params.append(f'%"{keyword}"%')

        if search_text:
            query += " AND post_content LIKE ?"
            params.append(f"%{search_text}%")

        query += " ORDER BY scraped_at DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def dismiss_lead(lead_id: int) -> bool:
    """Mark a lead as dismissed"""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE leads
                SET dismissed = 1
                WHERE id = ?
            """,
                (lead_id,),
            )
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        print(f"Error dismissing lead {lead_id}: {e}")
        return False


def get_leads_today_count() -> int:
    """Get count of leads scraped today"""
    from datetime import datetime, timedelta

    today = datetime.now().date()
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) FROM leads
            WHERE DATE(scraped_at) = DATE(?)
            AND (dismissed = 0 OR dismissed IS NULL)
        """,
            (today.isoformat(),),
        )
        return cursor.fetchone()[0]
