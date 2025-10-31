"""
Twitter PR Lead Scraper
Searches Twitter for PR-related keywords and saves all matching tweets
No GPT filtering - manual review in dashboard
"""

import json
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional

from twitter_client import TwitterClient
from database import (
    init_database,
    save_lead,
    save_activity_id,
    mark_activity_scraped,
)
from utils import extract_budget_mention

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration is missing or invalid"""

    pass


def load_config() -> Dict:
    """Load configuration from config.json"""
    try:
        with open("config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        raise ConfigurationError(
            "config.json not found. Copy config.example.json to config.json"
        )
    except json.JSONDecodeError as e:
        raise ConfigurationError(f"Invalid JSON in config.json: {e}")

    # Validate required sections
    if "keywords" not in config or not config["keywords"]:
        raise ConfigurationError("No keywords configured in config.json")

    return config


def get_date_range(hours: Optional[int] = None) -> tuple[date, date]:
    """Get date range for Twitter search

    Args:
        hours: Number of hours to look back from now (default: 24)

    Returns:
        Tuple of (start_date, end_date)
    """
    if not hours:
        hours = 24

    end_date = datetime.now().date()
    start_date = (datetime.now() - timedelta(hours=hours)).date()

    return start_date, end_date


def detect_matched_filters(
    post_content: str, config: Dict
) -> Dict[str, List[str]]:
    """Detect which keywords and categories matched in the tweet

    Args:
        post_content: Tweet text
        config: Configuration dictionary

    Returns:
        Dict with matched_keywords and matched_categories
    """
    post_lower = post_content.lower()

    matched_keywords = []
    matched_categories = []

    # Check PR keywords
    for keyword in config.get("keywords", []):
        if keyword.lower() in post_lower:
            matched_keywords.append(keyword)

    # Check CPG categories
    for category in config.get("industries", []):
        if category.lower() in post_lower:
            matched_categories.append(category)

    return {
        "matched_keywords": matched_keywords,
        "matched_categories": matched_categories,
    }


def build_lead_data_from_tweet(tweet_data: Dict, config: Dict) -> Dict:
    """Build lead data from tweet (pure function, no side effects)

    Args:
        tweet_data: Raw tweet data from Twitter client
        config: Configuration dictionary

    Returns:
        Lead data dictionary
    """
    # Detect which filters matched
    matches = detect_matched_filters(tweet_data.get("text", ""), config)

    # Extract budget mention if present
    budget_mention = extract_budget_mention(tweet_data.get("text", ""))

    # Build lead data
    return {
        "author_name": tweet_data.get("author_name", ""),
        "author_username": tweet_data.get("author_username", ""),
        "author_handle": tweet_data.get("author_username", ""),  # For compatibility
        "post_content": tweet_data.get("text", ""),
        "post_url": tweet_data.get("post_url", ""),
        "created_at": tweet_data.get("created_at", ""),
        "matched_keywords": json.dumps(matches["matched_keywords"]),
        "matched_categories": json.dumps(matches["matched_categories"]),
        "matched_roles": json.dumps([]),  # Twitter doesn't have job titles
        "budget_mention": budget_mention,
        "raw_data": tweet_data.get("raw_data", ""),
    }


def is_tweet_already_processed(tweet_id: str, post_url: str) -> bool:
    """Check if tweet has already been processed (has side effect: saves if new)

    Args:
        tweet_id: Tweet ID to check
        post_url: Tweet URL

    Returns:
        True if already processed, False if new (and saves it)
    """
    return not save_activity_id("twitter", tweet_id, post_url)


def search_twitter_for_leads(
    config: Dict,
    date_range_hours: Optional[int] = None,
    client: Optional[TwitterClient] = None,
) -> List[Dict]:
    """Search Twitter for PR leads using configured keywords

    Args:
        config: Configuration dictionary
        date_range_hours: Hours to look back (default from config or 24)
        client: TwitterClient instance (default: creates new one)

    Returns:
        List of tweet dictionaries
    """
    twitter_config = config.get("twitter", {})

    if not twitter_config.get("enabled", True):
        logger.warning("Twitter search is not enabled in config")
        return []

    keywords = config.get("keywords", [])
    if not keywords:
        logger.warning("No keywords configured")
        return []

    # Get date range
    if not date_range_hours:
        date_range_hours = config.get("monitoring", {}).get("date_range_hours", 24)

    start_date, end_date = get_date_range(date_range_hours)

    print(f"Searching Twitter for PR leads...")
    print(f"  Keywords: {len(keywords)}")
    print(f"  Date range: {start_date} to {end_date} ({date_range_hours}h)")
    print(f"  Max results per keyword: {twitter_config.get('max_results_per_keyword', 100)}")

    # Use provided client or create new one
    if client is None:
        client = TwitterClient()
    all_tweets = []
    seen_tweet_ids = set()

    # Search for each keyword
    for i, keyword in enumerate(keywords):
        print(f"\n  [{i+1}/{len(keywords)}] Searching: '{keyword}'")

        try:
            tweets = client.search_tweets(
                query=keyword,
                start_date=start_date,
                end_date=end_date,
                max_results=twitter_config.get("max_results_per_keyword", 100),
            )

            print(f"    Found {len(tweets)} tweets")

            # Deduplicate by tweet ID
            for tweet in tweets:
                tweet_id = tweet.get("id")
                if tweet_id and tweet_id not in seen_tweet_ids:
                    seen_tweet_ids.add(tweet_id)
                    all_tweets.append(tweet)

        except Exception as e:
            logger.error(f"Error searching keyword '{keyword}': {e}")
            print(f"    ERROR: {e}")
            continue

    print(f"\nTwitter search complete:")
    print(f"  Total tweets found: {len(all_tweets)}")

    return all_tweets


def main():
    """Main entry point for Twitter scraper"""
    print("Twitter PR Lead Scraper")
    print("=" * 60)

    try:
        init_database()
        config = load_config()
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
        return

    # Get date range from config
    date_range_hours = config.get("monitoring", {}).get("date_range_hours")

    # Search Twitter
    tweets = search_twitter_for_leads(config, date_range_hours)

    if not tweets:
        print("\nNo tweets found. Try adjusting keywords or date range.")
        return

    # Process and save tweets
    print(f"\nProcessing {len(tweets)} tweets...")

    saved_count = 0
    skipped_count = 0

    for i, tweet_data in enumerate(tweets):
        try:
            tweet_id = tweet_data.get("id")
            author = tweet_data.get("author_username", "unknown")

            if not tweet_id:
                logger.warning(f"Tweet {i+1} has no ID, skipping")
                skipped_count += 1
                continue

            # Check if already processed
            if is_tweet_already_processed(tweet_id, tweet_data.get("post_url", "")):
                skipped_count += 1
                print(f"  [{i+1}/{len(tweets)}] Skipped: @{author} (duplicate)")
                continue

            # Build lead data (pure function)
            lead_data = build_lead_data_from_tweet(tweet_data, config)

            # Save to database
            save_lead("twitter", tweet_id, lead_data)
            mark_activity_scraped("twitter", tweet_id)

            saved_count += 1
            print(f"  [{i+1}/{len(tweets)}] Saved: @{author}")

        except KeyError as e:
            logger.error(f"Missing required field in tweet {i+1}: {e}")
            print(f"  [{i+1}/{len(tweets)}] ERROR: Missing field {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing tweet {i+1}: {e}")
            print(f"  [{i+1}/{len(tweets)}] ERROR: {e}")
            continue

    print(f"\nScraping complete:")
    print(f"  Saved: {saved_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Total: {len(tweets)}")


if __name__ == "__main__":
    main()
