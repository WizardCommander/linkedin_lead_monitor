"""
TwitterAPI.io client for PR lead generation
Simplified synchronous version for keyword searching
"""

import os
import time
import requests
import logging
from datetime import date, datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv
from types_twitter import TweetId, TwitterUsername

load_dotenv()

logger = logging.getLogger(__name__)


class TwitterClient:
    """TwitterAPI.io client for searching tweets"""

    BASE_URL = "https://api.twitterapi.io"
    RATE_LIMIT_DELAY = 8  # seconds between requests (free tier: 1 req/5 sec + buffer)
    TWEETS_PER_PAGE = 20  # TwitterAPI.io returns 20 tweets per page
    REQUEST_TIMEOUT = 30  # seconds

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Twitter API client

        Args:
            api_key: twitterapi.io API key. If None, reads from TWITTER_API_KEY env var
        """
        self.api_key = api_key or os.getenv("TWITTER_API_KEY")
        if not self.api_key:
            raise ValueError("Twitter API key required. Set TWITTER_API_KEY in .env")

        self.headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        }

    def search_tweets(
        self,
        query: str,
        start_date: date,
        end_date: date,
        max_results: int = 100,
    ) -> List[Dict]:
        """
        Search for tweets using TwitterAPI.io advanced search

        Args:
            query: Search query string (e.g., "looking for PR agency")
            start_date: Start date for search
            end_date: End date for search
            max_results: Maximum number of tweets to return

        Returns:
            List of tweet dictionaries with fields:
                - id: Tweet ID
                - text: Tweet content
                - author_name: Author's display name
                - author_username: Author's @username
                - created_at: Tweet timestamp (ISO format)
                - post_url: URL to tweet
                - raw_data: Full tweet data (JSON string)
        """
        # Build query with date filters and exclude retweets
        formatted_query = (
            f"{query} "
            f"since:{start_date.strftime('%Y-%m-%d')} "
            f"until:{end_date.strftime('%Y-%m-%d')} "
            f"-is:retweet"
        )

        logger.info(f"Searching tweets: {formatted_query}")

        tweet_ids = self._fetch_tweet_ids(formatted_query, max_results)

        if not tweet_ids:
            logger.info("No tweets found")
            return []

        # Get full tweet data
        tweets = self._fetch_tweets_by_ids(tweet_ids)

        logger.info(f"Found {len(tweets)} tweets")
        return tweets

    def _fetch_tweet_ids(self, query: str, max_results: int) -> List[TweetId]:
        """Fetch tweet IDs from search endpoint"""
        tweet_ids: List[TweetId] = []
        cursor = ""
        pages_needed = (max_results + self.TWEETS_PER_PAGE - 1) // self.TWEETS_PER_PAGE

        for page_num in range(pages_needed):
            try:
                url = f"{self.BASE_URL}/twitter/tweet/advanced_search"
                params = {
                    "query": query,
                    "queryType": "Latest",
                    "cursor": cursor,
                }

                logger.debug(f"Fetching page {page_num + 1}/{pages_needed}")

                response = requests.get(
                    url, headers=self.headers, params=params, timeout=self.REQUEST_TIMEOUT
                )

                if response.status_code != 200:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    break

                data = response.json()

                # Collect tweet IDs from this page
                page_tweets = data.get("tweets", [])
                if not page_tweets:
                    logger.debug("No more tweets found")
                    break

                for tweet_data in page_tweets:
                    if len(tweet_ids) >= max_results:
                        break

                    tweet_id = tweet_data.get("id")
                    if tweet_id:
                        tweet_ids.append(TweetId(str(tweet_id)))

                logger.debug(f"Collected {len(page_tweets)} tweet IDs from page {page_num + 1}")

                # Check if there are more pages
                if not data.get("has_next_page", False):
                    logger.debug("No more pages available")
                    break

                cursor = data.get("next_cursor", "")
                if not cursor:
                    logger.debug("No next cursor available")
                    break

                # Rate limiting
                time.sleep(self.RATE_LIMIT_DELAY)

            except requests.HTTPError as e:
                logger.error(f"HTTP error fetching page {page_num + 1}: {e}")
                break
            except requests.Timeout as e:
                logger.error(f"Timeout fetching page {page_num + 1}: {e}")
                break
            except requests.RequestException as e:
                logger.error(f"Request error fetching page {page_num + 1}: {e}")
                break
            except (KeyError, ValueError) as e:
                logger.error(f"Data parsing error on page {page_num + 1}: {e}")
                break

        logger.info(f"Collected {len(tweet_ids)} tweet IDs")
        return tweet_ids

    def _fetch_tweets_by_ids(self, tweet_ids: List[TweetId]) -> List[Dict]:
        """Fetch full tweet data by IDs"""
        if not tweet_ids:
            return []

        try:
            url = f"{self.BASE_URL}/twitter/tweets"
            params = {"tweet_ids": ",".join(tweet_ids)}

            logger.debug(f"Fetching {len(tweet_ids)} tweets by IDs")

            response = requests.get(
                url, headers=self.headers, params=params, timeout=self.REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                return []

            data = response.json()

            # Process tweets from response
            tweet_list = data.get("tweets", [])
            if not tweet_list:
                logger.warning("No tweets returned from API")
                return []

            tweets = []
            for tweet_data in tweet_list:
                tweet = self._convert_tweet_data(tweet_data)
                if tweet:
                    tweets.append(tweet)

            logger.info(f"Successfully processed {len(tweets)} tweets")
            return tweets

        except requests.HTTPError as e:
            logger.error(f"HTTP error fetching tweets: {e}")
            return []
        except requests.Timeout as e:
            logger.error(f"Timeout fetching tweets: {e}")
            return []
        except requests.RequestException as e:
            logger.error(f"Request error fetching tweets: {e}")
            return []
        except (KeyError, ValueError) as e:
            logger.error(f"Data parsing error: {e}")
            return []

    def _convert_tweet_data(
        self, tweet_data: Dict, fallback_datetime: Optional[datetime] = None
    ) -> Optional[Dict]:
        """Convert TwitterAPI.io tweet data to our internal format

        Args:
            tweet_data: Raw tweet data from API
            fallback_datetime: Datetime to use if parsing fails (default: current time)
        """
        try:
            # Extract basic info
            tweet_id = TweetId(str(tweet_data.get("id", "")))
            text = tweet_data.get("text", "")

            # Extract author info
            author_info = tweet_data.get("author", {})
            author_name = author_info.get("name", "")
            author_username = TwitterUsername(author_info.get("userName", ""))

            # Extract timestamp
            created_at_str = tweet_data.get("createdAt", "") or tweet_data.get("created_at", "")

            # Parse Twitter date format: 'Tue Aug 27 19:42:18 +0000 2024'
            try:
                if created_at_str:
                    created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                    created_at_iso = created_at.isoformat()
                else:
                    fallback = fallback_datetime or datetime.now()
                    created_at_iso = fallback.isoformat()
            except (ValueError, TypeError) as e:
                logger.warning(f"Tweet {tweet_id} date parsing failed: {e}")
                fallback = fallback_datetime or datetime.now()
                created_at_iso = fallback.isoformat()

            # Build tweet URL
            post_url = f"https://twitter.com/{author_username}/status/{tweet_id}" if author_username else ""

            # Store full tweet data as JSON for dashboard display
            import json

            raw_data = json.dumps(tweet_data)

            return {
                "id": tweet_id,
                "text": text,
                "author_name": author_name,
                "author_username": author_username,
                "created_at": created_at_iso,
                "post_url": post_url,
                "raw_data": raw_data,
            }

        except Exception as e:
            logger.error(f"Error converting tweet data: {e}")
            return None
