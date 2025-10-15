import re
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
