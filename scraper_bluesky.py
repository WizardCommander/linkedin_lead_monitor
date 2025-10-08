import json
import os
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv
from database import init_database, save_lead
from utils import extract_budget_mention

load_dotenv()

# Constants
MAX_KEYWORDS_IN_QUERY = 20
BLUESKY_API_BASE = "https://bsky.social/xrpc"

class ConfigurationError(Exception):
    """Raised when configuration is missing or invalid"""
    pass

def load_config():
    """Load configuration from config.json"""
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise ConfigurationError(
            "config.json not found. Copy config.example.json to config.json"
        )

def authenticate_bluesky() -> Optional[str]:
    """Authenticate with BlueSky and return access token"""
    username = os.getenv("BLUESKY_USERNAME")
    password = os.getenv("BLUESKY_PASSWORD")

    if not username or not password:
        raise ConfigurationError(
            "BlueSky credentials not found. Add BLUESKY_USERNAME and BLUESKY_PASSWORD to your .env file"
        )

    url = f"{BLUESKY_API_BASE}/com.atproto.server.createSession"
    data = {
        "identifier": username,
        "password": password
    }

    try:
        print("🔐 Authenticating with BlueSky...")
        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()
        session = response.json()
        print("✅ Authentication successful")
        return session.get("accessJwt")
    except requests.exceptions.RequestException as e:
        print(f"❌ Authentication failed: {e}")
        return None

def extract_job_title_from_bio(bio: str) -> Optional[str]:
    """Try to extract job title from bio"""
    title_keywords = [
        "CMO", "Chief Marketing Officer", "Brand Manager", "Marketing Director",
        "Communications Manager", "VP Marketing", "Director of Marketing",
        "Head of Marketing", "Marketing Manager", "PR Manager"
    ]

    for keyword in title_keywords:
        if keyword.lower() in bio.lower():
            return keyword

    return None

def search_bluesky_by_keyword(keyword: str, access_token: str, limit: int = 100) -> List[Dict]:
    """Search BlueSky posts for a single keyword"""
    from datetime import datetime, timedelta

    # Filter to last 24 hours
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    query = f'{keyword} since:{yesterday}'

    url = f"{BLUESKY_API_BASE}/app.bsky.feed.searchPosts"
    params = {
        "q": query,
        "limit": limit,
        "sort": "latest"
    }
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        print(f"  🔍 Searching: {keyword}")

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        posts = data.get("posts", [])
        print(f"    ✅ Found {len(posts)} posts")

        results = []
        for post in posts:
            author = post.get("author", {})
            record = post.get("record", {})
            uri = post.get("uri", "")

            post_id_parts = uri.split("/")
            post_id = post_id_parts[-1] if post_id_parts else uri

            author_handle = author.get("handle", "")
            post_url = f"https://bsky.app/profile/{author_handle}/post/{post_id}"

            results.append({
                "post_id": post_id,
                "author_name": author.get("displayName", ""),
                "author_handle": author_handle,
                "author_bio": author.get("description", ""),
                "content": record.get("text", ""),
                "created_at": record.get("createdAt", ""),
                "post_url": post_url,
                "likes": post.get("likeCount", 0),
                "reposts": post.get("repostCount", 0),
                "replies": post.get("replyCount", 0),
                "raw": post,
                "keyword": keyword
            })

        return results

    except requests.exceptions.RequestException as e:
        print(f"    ❌ Error: {e}")
        return []

def search_bluesky(keywords: List[str], access_token: str, limit: int = 100) -> List[Dict]:
    """Search BlueSky posts for multiple keywords (last 24 hours each)"""
    all_results = []
    seen_post_ids = set()

    print(f"🔍 Searching BlueSky for {len(keywords)} keywords (last 24 hours)...\n")

    for keyword in keywords:
        results = search_bluesky_by_keyword(keyword, access_token, limit)

        # Deduplicate across keywords
        for post in results:
            post_id = post.get("post_id")
            if post_id not in seen_post_ids:
                all_results.append(post)
                seen_post_ids.add(post_id)

    print(f"\n✅ Total unique posts found: {len(all_results)}")
    return all_results

def process_bluesky_post(post: Dict) -> Dict:
    """Process a BlueSky post and extract relevant lead data"""
    content = post.get("content", "")
    bio = post.get("author_bio", "")

    budget = extract_budget_mention(content)
    job_title = extract_job_title_from_bio(bio)

    return {
        "author_name": post.get("author_name"),
        "author_handle": post.get("author_handle"),
        "author_title": job_title or bio[:100],
        "post_content": content,
        "post_url": post.get("post_url"),
        "budget_mention": budget,
        "created_at": post.get("created_at"),
        "raw_data": json.dumps(post.get("raw"))
    }

def main():
    print("📘 BlueSky PR Lead Scraper")
    print("=" * 60)

    try:
        init_database()
        config = load_config()
        access_token = authenticate_bluesky()
    except ConfigurationError as e:
        print(f"❌ Configuration Error: {e}")
        return

    if not access_token:
        print("❌ Failed to authenticate with BlueSky")
        return

    keywords = config.get("keywords", [])
    if not keywords:
        print("❌ No keywords found in config.json")
        return

    posts = search_bluesky(keywords, access_token, limit=100)

    if not posts:
        print("No results found.")
        return

    print(f"💾 Saving {len(posts)} posts to database...")

    saved_count = 0
    duplicate_count = 0

    for post in posts:
        lead_data = process_bluesky_post(post)
        post_id = post.get("post_id")

        if save_lead("bluesky", post_id, lead_data):
            saved_count += 1
        else:
            duplicate_count += 1

    print(f"\n✅ Saved {saved_count} new leads")
    if duplicate_count > 0:
        print(f"⏭️  Skipped {duplicate_count} duplicates")
    print(f"💾 Data saved to pr_leads.db")

    print("\n📊 Sample of results:")
    for i, post in enumerate(posts[:3]):
        print(f"\n  Post {i+1}:")
        print(f"    Author: {post.get('author_name')} (@{post.get('author_handle')})")
        print(f"    Content: {post.get('content')[:100]}...")
        print(f"    URL: {post.get('post_url')}")

if __name__ == "__main__":
    main()
