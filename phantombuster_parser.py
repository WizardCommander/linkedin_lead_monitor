"""
Parser for PhantomBuster LinkedIn content search output

Normalizes PhantomBuster data to our internal post format.
"""

import re
from typing import Dict, Any, List, Optional


def extract_activity_id_from_url(url: str) -> Optional[str]:
    """
    Extract LinkedIn activity ID from post URL

    Args:
        url: LinkedIn post URL (e.g., linkedin.com/posts/...-activity-7123456789)

    Returns:
        Activity ID string or None if not found

    Examples:
        >>> extract_activity_id_from_url("linkedin.com/posts/foo-activity-7123456")
        "7123456"
        >>> extract_activity_id_from_url("https://www.linkedin.com/feed/update/urn:li:activity:7123456")
        "7123456"
    """
    if not url:
        return None

    # Match activity ID from various LinkedIn URL formats
    patterns = [
        r"activity[:-](\d+)",  # /posts/foo-activity-7123 or activity:7123
        r"urn:li:activity:(\d+)",  # urn:li:activity:7123
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return None


def normalize_post_data(pb_post: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize PhantomBuster post data to our internal format

    Args:
        pb_post: Raw post dict from PhantomBuster output

    Returns:
        Normalized post dict with fields:
            - text: post content
            - activity_id: unique identifier
            - post_url: LinkedIn post URL
            - author_name: post author name
            - author_profile_url: author's LinkedIn profile
            - timestamp: post timestamp (if available)

    Examples:
        >>> pb_post = {
        ...     "postContent": "Looking for PR agency",
        ...     "postUrl": "linkedin.com/posts/foo-activity-7123",
        ...     "profileName": "John Doe",
        ...     "profileUrl": "linkedin.com/in/johndoe"
        ... }
        >>> result = normalize_post_data(pb_post)
        >>> result["text"]
        'Looking for PR agency'
        >>> result["activity_id"]
        '7123'
    """
    # PhantomBuster field names may vary; try common variations
    text = (
        pb_post.get("postContent")
        or pb_post.get("text")
        or pb_post.get("content")
        or pb_post.get("description")
        or ""
    )

    post_url = pb_post.get("postUrl") or pb_post.get("url") or ""

    # Extract activity ID from URL
    activity_id = extract_activity_id_from_url(post_url)

    # Fallback: use query if activity_id not found in URL
    if not activity_id:
        activity_id = pb_post.get("query", "")

    author_name = (
        pb_post.get("profileName")
        or pb_post.get("authorName")
        or pb_post.get("name")
        or ""
    )

    author_profile_url = (
        pb_post.get("profileUrl")
        or pb_post.get("authorUrl")
        or pb_post.get("profile")
        or ""
    )

    # Optional fields
    timestamp = pb_post.get("timestamp") or pb_post.get("date") or ""

    likes = pb_post.get("likes") or pb_post.get("likeCount") or 0
    comments = pb_post.get("comments") or pb_post.get("commentCount") or 0

    return {
        "text": text,
        "activity_id": activity_id,
        "post_url": post_url,
        "author_name": author_name,
        "author_profile_url": author_profile_url,
        "timestamp": timestamp,
        "likes": likes,
        "comments": comments,
        "source": "phantombuster",
    }


def parse_phantombuster_output(
    raw_output: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Parse full PhantomBuster output and normalize all posts

    Args:
        raw_output: List of raw post dicts from PhantomBuster

    Returns:
        List of normalized post dicts

    Examples:
        >>> raw = [{"postContent": "Looking for PR", "postUrl": "linkedin.com/posts/foo-activity-123"}]
        >>> results = parse_phantombuster_output(raw)
        >>> len(results)
        1
        >>> results[0]["source"]
        'phantombuster'
    """
    normalized_posts = []
    skipped_no_text = 0

    for i, raw_post in enumerate(raw_output):
        try:
            normalized = normalize_post_data(raw_post)

            # Skip posts with no text content
            if not normalized["text"]:
                skipped_no_text += 1
                if i < 3:  # Log first 3 skipped items to debug
                    print(f"      Skipped item {i} (no text). Keys: {list(raw_post.keys())[:10]}")
                continue

            normalized_posts.append(normalized)
        except Exception as e:
            # Log error but continue processing other posts
            print(f"Warning: Failed to parse post {i}: {e}")
            continue

    if skipped_no_text > 0:
        print(f"    Skipped {skipped_no_text} items with no text content")

    return normalized_posts
