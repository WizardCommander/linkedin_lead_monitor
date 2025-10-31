import re
from datetime import datetime, timedelta
from typing import Optional


def extract_budget_mention(text: str) -> Optional[str]:
    """Extract budget/retainer mentions from post text"""
    patterns = [
        r"\$[\d,]+(?:k|K)?(?:\s*(?:-|to)\s*\$[\d,]+(?:k|K)?)?",
        r"budget.*?\$[\d,]+",
        r"retainer.*?\$[\d,]+",
        r"[\d,]+k?\s*(?:per|/)\s*month",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


def parse_date_range(range_string: str) -> Optional[int]:
    """Parse a date range string to hours

    Args:
        range_string: String like "24h", "7d", "1w", "1m"

    Returns:
        Number of hours, or None if invalid
    """
    if not range_string:
        return None

    # Match patterns like "24h", "7d", "1w", "1m"
    match = re.match(r"^(\d+)([hdwm])$", range_string.lower())
    if not match:
        return None

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "h":
        return value
    elif unit == "d":
        return value * 24
    elif unit == "w":
        return value * 24 * 7
    elif unit == "m":
        return value * 24 * 30

    return None


def format_relative_time(timestamp: str) -> str:
    """Format a timestamp as relative time (e.g., "2 hours ago")

    Args:
        timestamp: ISO format timestamp string

    Returns:
        Human-readable relative time string
    """
    if not timestamp:
        return "unknown time"

    try:
        # Parse ISO timestamp
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        # Calculate time difference
        now = datetime.now()
        # Make both timezone-aware or both naive for comparison
        if dt.tzinfo is not None:
            # dt is timezone-aware, make now aware too (assume UTC)
            from datetime import timezone
            now = now.replace(tzinfo=timezone.utc)

        time_diff = now - dt

        # Format based on duration
        seconds = int(time_diff.total_seconds())

        if seconds < 60:
            return "just now"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif seconds < 604800:
            days = seconds // 86400
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif seconds < 2592000:
            weeks = seconds // 604800
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        else:
            months = seconds // 2592000
            return f"{months} month{'s' if months != 1 else ''} ago"

    except (ValueError, AttributeError):
        return "unknown time"
