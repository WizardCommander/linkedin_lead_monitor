"""Type definitions for Twitter scraper"""

from typing import NewType

# Branded types for IDs (C-5 MUST)
TweetId = NewType("TweetId", str)
TwitterUsername = NewType("TwitterUsername", str)
