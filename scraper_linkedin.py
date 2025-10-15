import json
import re
import os
import logging
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv
from apify_client import ApifyClient
from openai import OpenAI
from database import (
    init_database,
    save_lead,
    save_activity_id,
    mark_activity_scraped,
)
from utils import extract_budget_mention

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Constants
MAX_KEYWORDS_IN_QUERY = 15
MAX_JOB_TITLES_IN_QUERY = 7
MAX_INDUSTRIES_IN_QUERY = 7


class ConfigurationError(Exception):
    """Raised when configuration is missing or invalid"""

    pass


def validate_keyword_format(keywords: List[str]) -> List[str]:
    """Validate keyword format for exact phrase search strategy

    Args:
        keywords: List of keyword strings (should be quoted for exact matching)

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    for i, keyword in enumerate(keywords):
        if not isinstance(keyword, str):
            errors.append(
                f"Keyword {i+1} must be a string, got {type(keyword).__name__}"
            )
            continue

        # Check that keyword is properly quoted for exact search
        if not (keyword.startswith('"') and keyword.endswith('"')):
            errors.append(
                f"Keyword {i+1} '{keyword}' must be enclosed in quotes for exact phrase matching"
            )
            continue

        # Check for reasonable length (strip quotes for length check)
        content = keyword.strip('"')
        if len(content) < 3:
            errors.append(
                f"Keyword {i+1} '{keyword}' content is too short (minimum 3 characters inside quotes)"
            )

        if len(content) > 100:
            errors.append(
                f"Keyword {i+1} '{keyword}' content is too long (maximum 100 characters inside quotes)"
            )

        # Check for empty keyword content
        if not content.strip():
            errors.append(f"Keyword {i+1} '{keyword}' cannot be empty inside quotes")

    return errors


def validate_config_structure(config: dict) -> List[str]:
    """Validate configuration structure and content

    Args:
        config: Configuration dictionary

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Required sections
    required_sections = ["keywords", "job_titles", "industries", "apify"]
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required configuration section: {section}")

    # Validate keywords
    if "keywords" in config:
        if not isinstance(config["keywords"], list):
            errors.append("Keywords must be a list")
        elif len(config["keywords"]) == 0:
            errors.append("At least one keyword is required")
        else:
            keyword_errors = validate_keyword_format(config["keywords"])
            errors.extend(keyword_errors)

    # Validate apify settings
    if "apify" in config:
        apify_config = config["apify"]
        if not isinstance(apify_config, dict):
            errors.append("Apify configuration must be an object")
        else:
            # Check required apify settings
            required_apify = ["results_per_keyword", "date_filter", "sort_type"]
            for setting in required_apify:
                if setting not in apify_config:
                    errors.append(f"Missing required apify setting: {setting}")

            # Validate specific values
            if "results_per_keyword" in apify_config:
                if (
                    not isinstance(apify_config["results_per_keyword"], int)
                    or apify_config["results_per_keyword"] < 1
                ):
                    errors.append("results_per_keyword must be a positive integer")

            if "date_filter" in apify_config:
                valid_filters = ["past-24h", "past-week", "past-month"]
                if apify_config["date_filter"] not in valid_filters:
                    errors.append(f"date_filter must be one of: {valid_filters}")

    return errors


def load_config():
    """Load configuration from .env and config.json"""
    config = {}

    # Load API credentials from .env
    config["google_api_key"] = os.getenv("GOOGLE_SEARCH_API_KEY")
    config["google_cx_id"] = os.getenv("SEARCH_ENGINE_ID")
    config["apify_api_token"] = os.getenv("APIFY_API_TOKEN")

    # Apify is now required, Google is optional (legacy)
    if not config["apify_api_token"]:
        raise ConfigurationError(
            "Missing Apify API token in .env file. "
            "Add APIFY_API_TOKEN to your .env file. "
            "Get your token from https://console.apify.com/account/integrations"
        )

    # Load keywords from config.json
    try:
        with open("config.json", "r") as f:
            json_config = json.load(f)
            config.update(json_config)
    except FileNotFoundError:
        raise ConfigurationError(
            "config.json not found. Copy config.example.json to config.json"
        )
    except json.JSONDecodeError as e:
        raise ConfigurationError(f"Invalid JSON in config.json: {e}")

    # Validate configuration structure and content
    validation_errors = validate_config_structure(config)
    if validation_errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(
            f"  - {error}" for error in validation_errors
        )
        raise ConfigurationError(error_msg)

    return config


def get_apify_linkedin_posts(config: Dict) -> List[Dict]:
    """Get LinkedIn posts data using Apify API

    Uses Apify's LinkedIn Posts Search Scraper to find posts matching keywords.
    Returns complete post data including content, author info, etc.

    Args:
        config: Configuration dictionary with Apify credentials and search settings

    Returns:
        List of post data dictionaries with URLs, content, author info, etc.
    """
    client = ApifyClient(config["apify_api_token"])
    all_urls = []
    total_results_count = 0

    keywords = config.get("keywords", [])
    if not keywords:
        logger.warning("No keywords configured")
        return []

    apify_config = config.get("apify", {})
    results_per_keyword = apify_config.get("results_per_keyword", 3)

    # Validate result limit (Apify max is 50)
    if results_per_keyword < 1 or results_per_keyword > 50:
        logger.warning(
            f"results_per_keyword ({results_per_keyword}) out of range [1-50], clamping"
        )
        results_per_keyword = max(1, min(50, results_per_keyword))

    # Validate date filter (Apify only accepts specific values)
    VALID_DATE_FILTERS = ["", "past-24h", "past-week", "past-month"]
    date_filter = apify_config.get("date_filter", "past-24h")
    if date_filter not in VALID_DATE_FILTERS:
        logger.warning(
            f"Invalid date_filter '{date_filter}', using 'past-24h'. "
            f"Valid values: {VALID_DATE_FILTERS}"
        )
        date_filter = "past-24h"

    sort_type = apify_config.get("sort_type", "date_posted")
    use_job_title_filter = apify_config.get("use_job_title_filter", True)

    # Build job titles filter string
    job_titles_str = None
    if use_job_title_filter:
        job_titles = config.get("job_titles", [])
        if job_titles:
            job_titles_str = ", ".join(job_titles)

    print(f"Searching LinkedIn via Apify...")
    print(f"  Keywords: {len(keywords)}")
    print(f"  Results per keyword: {results_per_keyword}")
    print(f"  Date filter: {date_filter}")
    if job_titles_str:
        print(f"  Job title filter: {job_titles_str[:100]}...")

    # Search each keyword individually for best results
    for i, keyword in enumerate(keywords):
        print(f"\n  [{i+1}/{len(keywords)}] Searching: '{keyword}'")

        # Prepare actor input - use snake_case as per Apify API
        run_input = {
            "keyword": keyword,
            "sort_type": sort_type,
            "date_filter": date_filter,
            "limit": results_per_keyword,
            "page_number": 1,
        }

        # Add job title filter if enabled
        if job_titles_str:
            run_input["author_job_title"] = job_titles_str

        try:
            # Run the Apify actor
            run = client.actor(
                "apimaestro/linkedin-posts-search-scraper-no-cookies"
            ).call(run_input=run_input)

            # Fetch results
            results = []
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)

            print(f"    Found {len(results)} results")

            total_results_count += len(results)

            # Extract URLs and post data from results
            for item in results:
                # Apify returns post data with post_url field
                url = (
                    item.get("post_url")
                    or item.get("url")
                    or item.get("postUrl")
                    or item.get("link")
                )
                # Accept LinkedIn post URLs (format: linkedin.com/posts/username-...)
                if url and "linkedin.com/posts/" in url:
                    # Store the complete Apify data for later use
                    apify_data = {
                        "url": url,
                        "activity_id": item.get("activity_id"),
                        "text": item.get("text", ""),
                        "author": item.get("author", {}),
                        "posted_at": item.get("posted_at"),
                        "hashtags": item.get("hashtags", []),
                        "stats": item.get("stats", {}),
                        "is_reshare": item.get("is_reshare", False),
                        "metadata": item.get("metadata", {}),
                        "search_input": keyword,  # Track which keyword found this post
                    }
                    all_urls.append(apify_data)

        except Exception as e:
            print(f"    ERROR: Error searching keyword '{keyword}': {e}")
            logger.error(f"Apify error for keyword '{keyword}': {e}")
            continue

    # Remove duplicates by activity_id (same post might match multiple keywords)
    seen_activity_ids = set()
    unique_posts = []
    for post_data in all_urls:
        activity_id = post_data.get("activity_id")
        if activity_id and activity_id not in seen_activity_ids:
            seen_activity_ids.add(activity_id)
            unique_posts.append(post_data)
        elif not activity_id:
            # Fallback for posts without activity_id - use URL
            url = post_data.get("url")
            if url not in seen_activity_ids:
                seen_activity_ids.add(url)
                unique_posts.append(post_data)
    all_urls = unique_posts

    print(f"\nApify search complete:")
    print(f"  Total results fetched: {total_results_count}")
    print(f"  Unique LinkedIn URLs: {len(all_urls)}")
    print(f"  Estimated cost: ${total_results_count / 1000 * 5:.2f}")

    return all_urls


def extract_activity_id(url: str) -> Optional[str]:
    """Extract activity ID from LinkedIn post URL

    Handles Apify URL format: linkedin.com/posts/username-7380301291354263553-tCfn
    Activity IDs are always 19 digits.

    Args:
        url: LinkedIn post URL from Apify

    Returns:
        19-digit activity ID, or None if not found
    """
    # Extract 19-digit activity ID from /posts/ path segment
    # Format: /posts/username-ACTIVITYID-hash or /posts/username-ACTIVITYID
    match = re.search(r"/posts/[^/]*?-(\d{19})(?:-|$)", url)
    if match:
        return match.group(1)

    # Fallback: any 19-digit number (less precise but handles edge cases)
    match = re.search(r"(\d{19})", url)
    if match:
        logger.debug(f"Extracted activity ID using fallback pattern: {match.group(1)}")
        return match.group(1)

    logger.warning(f"Could not extract activity ID from URL: {url}")
    return None


def extract_company_from_title(title: Optional[str]) -> Optional[str]:
    """Extract company name from author title

    Parses patterns like:
    - "CMO at Acme Corp" → "Acme Corp"
    - "Marketing Director @ Beauty Co" → "Beauty Co"
    - "VP Marketing | Food Inc" → "Food Inc"

    Args:
        title: Author title string (e.g., "CMO at Acme Corp")

    Returns:
        Company name if found, None otherwise
    """
    if not title or not isinstance(title, str):
        return None

    title = title.strip()
    if not title:
        return None

    # Try "at" separator first (case-insensitive)
    at_match = re.search(r"\bat\b(.+)", title, re.IGNORECASE)
    if at_match:
        return at_match.group(1).strip()

    # Try "@" separator
    if "@" in title:
        parts = title.split("@", 1)
        if len(parts) == 2:
            return parts[1].strip()

    # Try "|" separator
    if "|" in title:
        parts = title.split("|", 1)
        if len(parts) == 2:
            return parts[1].strip()

    return None


def validate_apify_post(post_data: dict) -> bool:
    """Validate that Apify post data has required fields

    Args:
        post_data: Raw post data from Apify

    Returns:
        True if post has all required fields, False otherwise
    """
    if not isinstance(post_data, dict):
        return False

    required_fields = ["activity_id", "url", "text", "author"]
    return all(
        field in post_data and post_data[field] is not None for field in required_fields
    )


def extract_lead_data(post_data: dict) -> dict:
    """Extract and normalize lead data from Apify post

    Args:
        post_data: Validated Apify post data

    Returns:
        Normalized lead data dictionary
    """
    author = post_data.get("author", {})

    # Handle author field - can be dict or string depending on Apify version
    if isinstance(author, dict):
        author_name = author.get("name", "Unknown")
        author_title = author.get("headline", "")
    elif isinstance(author, str):
        author_name = author
        author_title = ""
    else:
        author_name = "Unknown"
        author_title = ""

    return {
        "activity_id": post_data["activity_id"],
        "url": post_data["url"],
        "post_url": post_data["url"],  # Database expects post_url field
        "author_name": author_name,
        "author_title": author_title,
        "post_content": post_data.get("text", ""),
        "posted_at": post_data.get("posted_at"),
        "hashtags": post_data.get("hashtags", []),
        "stats": post_data.get("stats", {}),
        "search_input": post_data.get("search_input"),
    }


def filter_post_by_keywords(post_data: dict, config: dict) -> bool:
    """Check if post matches any configured keywords

    Args:
        post_data: Apify post data
        config: Configuration with keywords

    Returns:
        True if post matches keywords, False otherwise
    """
    post_content = post_data.get("text", "").lower()
    keywords = config.get("keywords", [])

    # Debug logging for filtered posts only
    activity_id = post_data.get("activity_id", "unknown")

    matches = []
    for keyword in keywords:
        # Strip quotes from keyword for matching (defensive for any quoted keywords)
        clean_keyword = keyword.strip('"').lower()

        # More flexible matching: check if any significant words from the keyword appear
        keyword_words = [
            word for word in clean_keyword.split() if len(word) > 2
        ]  # Skip short words like "a", "of"

        # If any meaningful word from the keyword is found, consider it a match
        if any(word in post_content for word in keyword_words):
            matches.append(keyword)

    result = len(matches) > 0

    # Only log if no matches (to debug what's being filtered out)
    if not result:
        logger.info(f"FILTERED OUT {activity_id}: '{post_content[:150]}...'")
        logger.info(f"  Search term: {post_data.get('search_input', 'unknown')}")

    return result


# Global circuit breaker state
GPT_FAILURE_COUNT = 0
GPT_CIRCUIT_BREAKER_THRESHOLD = 5
GPT_CIRCUIT_BREAKER_ACTIVE = False

# Cost monitoring
GPT_DAILY_CALL_COUNT = 0
GPT_DAILY_CALL_LIMIT = 1000  # Configurable daily limit
GPT_LAST_RESET_DATE = None


def validate_gpt_response(response: dict) -> bool:
    """Validate GPT response has required fields and proper types

    Args:
        response: GPT response dictionary

    Returns:
        True if response is valid, False otherwise
    """
    required_fields = [
        "is_genuine_lead",
        "confidence_score",
        "lead_quality",
        "hiring_type",
        "reasoning",
    ]

    # Check required fields exist
    if not all(field in response for field in required_fields):
        return False

    # Validate field types and values
    try:
        # Boolean validation
        if not isinstance(response["is_genuine_lead"], bool):
            return False

        # Score validation (0-100)
        score = response["confidence_score"]
        if not isinstance(score, (int, float)) or not 0 <= score <= 100:
            return False

        # Lead quality validation
        if response["lead_quality"] not in ["hot", "warm", "cold"]:
            return False

        # Hiring type validation
        if response["hiring_type"] not in ["agency", "in-house", "unclear"]:
            return False

        # Reasoning validation
        if (
            not isinstance(response["reasoning"], str)
            or len(response["reasoning"]) < 10
        ):
            return False

        return True

    except (KeyError, TypeError, ValueError):
        return False


def reset_gpt_circuit_breaker():
    """Reset circuit breaker after successful GPT calls"""
    global GPT_FAILURE_COUNT, GPT_CIRCUIT_BREAKER_ACTIVE
    GPT_FAILURE_COUNT = 0
    GPT_CIRCUIT_BREAKER_ACTIVE = False


def check_gpt_circuit_breaker() -> bool:
    """Check if circuit breaker should block GPT calls

    Returns:
        True if GPT calls should be blocked, False if allowed
    """
    global GPT_FAILURE_COUNT, GPT_CIRCUIT_BREAKER_ACTIVE, GPT_CIRCUIT_BREAKER_THRESHOLD

    if GPT_FAILURE_COUNT >= GPT_CIRCUIT_BREAKER_THRESHOLD:
        GPT_CIRCUIT_BREAKER_ACTIVE = True
        return True

    return False


def record_gpt_failure():
    """Record a GPT API failure for circuit breaker tracking"""
    global GPT_FAILURE_COUNT
    GPT_FAILURE_COUNT += 1
    logger.warning(
        f"GPT API failure recorded. Count: {GPT_FAILURE_COUNT}/{GPT_CIRCUIT_BREAKER_THRESHOLD}"
    )


def check_daily_gpt_limit() -> bool:
    """Check if daily GPT call limit has been reached

    Returns:
        True if limit reached, False if calls allowed
    """
    global GPT_DAILY_CALL_COUNT, GPT_DAILY_CALL_LIMIT, GPT_LAST_RESET_DATE
    import datetime

    today = datetime.date.today()

    # Reset counter if it's a new day
    if GPT_LAST_RESET_DATE != today:
        GPT_DAILY_CALL_COUNT = 0
        GPT_LAST_RESET_DATE = today
        logger.info(f"Reset daily GPT call count for {today}")

    # Check if limit reached
    if GPT_DAILY_CALL_COUNT >= GPT_DAILY_CALL_LIMIT:
        logger.warning(
            f"Daily GPT call limit reached: {GPT_DAILY_CALL_COUNT}/{GPT_DAILY_CALL_LIMIT}"
        )
        return True

    return False


def record_gpt_call():
    """Record a successful GPT API call for cost monitoring"""
    global GPT_DAILY_CALL_COUNT
    GPT_DAILY_CALL_COUNT += 1

    # Log milestones
    if GPT_DAILY_CALL_COUNT % 100 == 0:
        logger.info(f"GPT calls today: {GPT_DAILY_CALL_COUNT}/{GPT_DAILY_CALL_LIMIT}")


def get_gpt_usage_stats() -> dict:
    """Get current GPT usage statistics

    Returns:
        Dictionary with usage stats
    """
    return {
        "daily_calls": GPT_DAILY_CALL_COUNT,
        "daily_limit": GPT_DAILY_CALL_LIMIT,
        "circuit_breaker_active": GPT_CIRCUIT_BREAKER_ACTIVE,
        "failure_count": GPT_FAILURE_COUNT,
        "last_reset_date": str(GPT_LAST_RESET_DATE) if GPT_LAST_RESET_DATE else None,
    }


def analyze_lead_with_gpt(
    post_content: str, author_name: str, author_title: str, post_url: str
) -> dict:
    """Use GPT to analyze if this is a genuine PR lead for BDPR"""

    # Target data from BDPR requirements PDF
    target_keywords = [
        "affiliate PR",
        "commerce PR",
        "agency recommendations",
        "any PR agency recs",
        "beauty PR agency",
        "brand launch PR",
        "brand manager needs PR support",
        "CMO looking for PR",
        "comms manager seeking agency",
        "consumer brand PR",
        "CPG PR agency",
        "crisis comms agency",
        "DTC PR",
        "direct-to-consumer PR",
        "food & beverage PR",
        "hiring a PR agency",
        "launch PR agency",
        "looking for a PR agency",
        "need a PR firm",
        "PR partner needed",
        "recommend a PR agency",
        "RFP for PR",
        "PR RFP",
        "seeking PR support",
    ]

    target_roles = [
        "Brand Manager",
        "Senior Brand Manager",
        "Chief Marketing Officer",
        "CMO",
        "Communications Manager",
        "Director of Marketing",
        "Director of Brand Marketing",
        "Director of Corporate Communications",
        "Head of Communications",
        "Head of PR",
        "Marketing Manager",
        "Senior Marketing Manager",
        "VP Marketing",
        "Vice President of Marketing",
        "Brand Communications Specialist",
    ]

    target_industries = [
        "Beauty",
        "Food and beverage",
        "CPG",
        "Consumer goods",
        "DTC",
        "Health and wellness",
        "Personal care",
        "Skincare",
        "Haircare",
        "Makeup",
        "Fragrance",
        "Apparel",
        "Fashion",
        "Baby products",
        "Pet products",
        "Supplements",
        "Beverages",
        "Coffee",
        "Tea",
        "Energy drinks",
        "Functional beverages",
        "Organic",
        "Natural products",
    ]

    prompt = f"""Analyze this LinkedIn post to determine if the author is a GENUINE LEAD for a PR agency (BDPR).

POST CONTENT: "{post_content}"
AUTHOR: {author_name}
AUTHOR TITLE: {author_title}

EVALUATION CRITERIA (be PERMISSIVE - when in doubt, qualify as lead):

1. AGENCY vs IN-HOUSE HIRING (PRIMARY FILTER):
   - Are they seeking an EXTERNAL PR AGENCY/FIRM? ✅ ACCEPT
   - Or looking to hire IN-HOUSE employees? ❌ REJECT
   - Key signals: "agency", "firm", "partner", "vendor" vs "hire", "employee", "join our team"

2. AUTHOR LEGITIMACY (FLEXIBLE):
   - Decision makers (CEO, Founder, CMO, Marketing roles) ✅ ACCEPT
   - PREFERRED roles: {target_roles}
   - But also accept other business decision makers
   - REJECT: PR professionals/agencies offering services ❌

3. BUSINESS RELEVANCE (BROAD):
   - Any business needing PR services ✅ ACCEPT
   - IDEAL industries: {target_industries}
   - But don't reject other businesses unless clearly B2B services

4. INTENT SIGNALS (PERMISSIVE):
   - Any mention of PR/communications needs ✅ ACCEPT
   - Keywords help: {target_keywords}
   - Include posts about: launches, funding, crises, growth, rebranding

5. CLEAR REJECTIONS ONLY:
   - PR agencies offering services ❌
   - In-house job postings ❌
   - Obvious spam/irrelevant content ❌

BIAS TOWARD ACCEPTANCE: If there's any reasonable chance this is a potential client, mark as genuine lead.

Respond with JSON only:
{{
    "is_genuine_lead": true/false,
    "confidence_score": 0-100,
    "lead_quality": "hot/warm/cold",
    "hiring_type": "agency/in-house/unclear",
    "reasoning": "2-3 sentence explanation",
    "urgency_indicators": ["specific", "signals"],
    "industry_match": "specific industry if identifiable",
    "target_role_match": true/false,
    "budget_mentions": ["any budget/timeline hints"],
    "red_flags": ["any concerning signals"]
}}"""

    # Check circuit breaker
    if check_gpt_circuit_breaker():
        logger.warning("GPT circuit breaker active - skipping GPT analysis")
        return create_fallback_response("Circuit breaker active")

    # Check daily limit
    if check_daily_gpt_limit():
        logger.warning("Daily GPT limit reached - skipping GPT analysis")
        return create_fallback_response("Daily limit reached")

    # Retry logic with exponential backoff
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            import time

            # Add delay for retries (exponential backoff)
            if attempt > 0:
                delay = base_delay * (2 ** (attempt - 1))
                logger.info(f"GPT retry attempt {attempt + 1} after {delay}s delay")
                time.sleep(delay)

            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at analyzing LinkedIn posts to identify genuine PR agency leads. Always respond with valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=500,
                timeout=30.0,  # Add timeout to prevent hanging
            )

            # Parse and validate response
            response_content = response.choices[0].message.content.strip()

            # Clean potential markdown code blocks
            if response_content.startswith("```json"):
                response_content = response_content[7:]
            if response_content.endswith("```"):
                response_content = response_content[:-3]

            result = json.loads(response_content)

            # Validate response structure
            if not validate_gpt_response(result):
                raise ValueError("GPT response failed validation")

            # Success - reset circuit breaker and record call
            reset_gpt_circuit_breaker()
            record_gpt_call()
            logger.debug("GPT analysis successful")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"GPT returned invalid JSON (attempt {attempt + 1}): {e}")
            record_gpt_failure()
            if attempt == max_retries - 1:
                return create_fallback_response(
                    f"Invalid JSON after {max_retries} attempts"
                )

        except Exception as e:
            logger.error(f"GPT API error (attempt {attempt + 1}): {e}")
            record_gpt_failure()

            # Check if this is a rate limit error (don't retry immediately)
            if "rate_limit" in str(e).lower():
                logger.warning("Rate limit detected - backing off")
                if attempt < max_retries - 1:
                    time.sleep(60)  # Wait 1 minute for rate limit

            if attempt == max_retries - 1:
                return create_fallback_response(
                    f"API error after {max_retries} attempts: {e}"
                )

    # Should never reach here, but just in case
    return create_fallback_response("Unexpected retry loop exit")


def create_fallback_response(reason: str) -> dict:
    """Create a fallback response when GPT analysis fails

    Args:
        reason: Reason for fallback

    Returns:
        Fallback response dictionary
    """
    return {
        "is_genuine_lead": True,  # Default to true to avoid missing leads
        "confidence_score": 50,
        "lead_quality": "warm",
        "hiring_type": "unclear",
        "reasoning": f"GPT analysis unavailable: {reason}",
        "urgency_indicators": [],
        "industry_match": "unknown",
        "target_role_match": False,
        "budget_mentions": [],
        "red_flags": ["GPT analysis unavailable"],
    }


def log_keyword_filter_result(post_data: dict, config: dict) -> bool:
    """Check if post passes keyword filtering with proper logging

    Args:
        post_data: Raw Apify post data
        config: Configuration dictionary

    Returns:
        True if post passes keyword filter, False otherwise
    """
    if not filter_post_by_keywords(post_data, config):
        # Get author name for logging (extract safely)
        author = post_data.get("author", {})
        if isinstance(author, dict):
            author_name = author.get("name", "Unknown")
        else:
            author_name = str(author) if author else "Unknown"

        activity_id = post_data.get("activity_id", "unknown")
        logger.info(f"Post {activity_id} from {author_name} has no matching keywords")
        return False
    return True


def enrich_lead_data(lead_data: dict, config: dict) -> dict:
    """Enrich lead data with computed fields and filter matches

    Args:
        lead_data: Basic lead data extracted from post
        config: Configuration dictionary

    Returns:
        Enriched lead data with all computed fields
    """
    # Detect which filters matched
    matches = detect_matched_filters(
        lead_data["post_content"], lead_data["author_title"], config
    )

    # Extract company from author title
    company = extract_company_from_title(lead_data["author_title"])

    # Add computed fields with JSON serialization for lists
    enriched_data = lead_data.copy()
    enriched_data.update(
        {
            "platform": "linkedin",
            "company": company,
            "matched_keywords": json.dumps(matches["matched_keywords"]),
            "matched_roles": json.dumps(matches["matched_roles"]),
            "matched_categories": json.dumps(matches["matched_categories"]),
        }
    )

    return enriched_data


def process_apify_post(post_data: dict, config: dict) -> Optional[dict]:
    """Process single Apify post into lead data with filtering

    Args:
        post_data: Raw Apify post data
        config: Configuration dictionary

    Returns:
        Lead data dict if post passes filters, None otherwise
    """
    # Validate required fields
    if not validate_apify_post(post_data):
        logger.warning(f"Invalid post data structure: {list(post_data.keys())}")
        return None

    # Extract normalized data
    lead_data = extract_lead_data(post_data)

    # Apply keyword filter first (quick filter)
    if not log_keyword_filter_result(post_data, config):
        return None

    # Show GPT analysis input for debugging
    author_name = lead_data.get("author_name", "Unknown")
    print(f"    [GPT ANALYZING] {author_name}")
    print(f"      Author Title: {lead_data.get('author_title', 'N/A')}")
    print(f"      Post Content: {lead_data.get('post_content', '')[:150]}{'...' if len(lead_data.get('post_content', '')) > 150 else ''}")

    # Apply GPT analysis for qualified posts
    gpt_analysis = analyze_lead_with_gpt(
        lead_data["post_content"],
        lead_data["author_name"],
        lead_data["author_title"],
        lead_data["url"],
    )

    # Show detailed GPT analysis results
    print(f"    [GPT RESULT] Analysis complete:")
    print(f"      Decision: {'✅ ACCEPT' if gpt_analysis.get('is_genuine_lead', False) else '❌ REJECT'}")
    print(f"      Confidence: {gpt_analysis.get('confidence_score', 'N/A')}%")
    print(f"      Lead Quality: {gpt_analysis.get('lead_quality', 'N/A')}")
    print(f"      Hiring Type: {gpt_analysis.get('hiring_type', 'N/A')}")
    print(f"      Industry Match: {gpt_analysis.get('industry_match', 'N/A')}")
    print(f"      Target Role Match: {gpt_analysis.get('target_role_match', 'N/A')}")
    print(f"      Reasoning: {gpt_analysis.get('reasoning', 'No reason provided')}")

    urgency = gpt_analysis.get('urgency_indicators', [])
    if urgency:
        print(f"      Urgency Indicators: {', '.join(urgency)}")

    red_flags = gpt_analysis.get('red_flags', [])
    if red_flags:
        print(f"      Red Flags: {', '.join(red_flags)}")

    # Filter out non-genuine leads based on GPT analysis
    if not gpt_analysis.get("is_genuine_lead", False):
        activity_id = lead_data.get("activity_id", "unknown")
        print(f"    [FILTERED] Post rejected by GPT analysis")
        logger.info(f"GPT FILTERED OUT {activity_id}: {gpt_analysis.get('reasoning', 'No reason provided')}")
        return None

    # Enrich with computed fields and GPT analysis results
    enriched_data = enrich_lead_data(lead_data, config)

    # Show successful GPT qualification
    print(f"    [QUALIFIED] ✅ Lead accepted for database save")
    print(f"      Final Decision: GENUINE LEAD")

    # Add GPT analysis results to the lead data
    enriched_data.update(
        {
            "gpt_confidence_score": gpt_analysis.get("confidence_score", 0),
            "gpt_lead_quality": gpt_analysis.get("lead_quality", "unknown"),
            "gpt_hiring_type": gpt_analysis.get("hiring_type", "unclear"),
            "gpt_reasoning": gpt_analysis.get("reasoning", ""),
            "gpt_urgency_indicators": json.dumps(
                gpt_analysis.get("urgency_indicators", [])
            ),
            "gpt_industry_match": gpt_analysis.get("industry_match", ""),
            "gpt_target_role_match": gpt_analysis.get("target_role_match", False),
            "gpt_budget_mentions": json.dumps(gpt_analysis.get("budget_mentions", [])),
            "gpt_red_flags": json.dumps(gpt_analysis.get("red_flags", [])),
        }
    )

    return enriched_data


def process_apify_posts_batch(apify_posts: List[Dict], config: Dict) -> int:
    """Process batch of Apify posts with error handling and rollback

    Args:
        apify_posts: List of post data from Apify
        config: Configuration dictionary

    Returns:
        Number of successfully processed posts
    """
    print(f"Processing {len(apify_posts)} posts from Apify data (no Playwright needed)")

    processed_count = 0
    failed_activity_ids = []

    for i, post_data in enumerate(apify_posts):
        try:
            activity_id = post_data.get("activity_id")
            url = post_data.get("post_url") or post_data.get("url")

            # Extract activity_id from URL if not provided by Apify
            if not activity_id:
                activity_id = extract_activity_id(url)

            if not activity_id:
                logger.warning(f"Could not extract activity_id from post: {url}")
                continue

            # Save activity_id to database (marks as discovered)
            if not save_activity_id("linkedin", activity_id, url):
                # Already exists, skip processing
                continue

            # Normalize field names for validation
            normalized_post_data = post_data.copy()
            if "post_url" in post_data and "url" not in post_data:
                normalized_post_data["url"] = post_data["post_url"]

            # Process the post with validation and filtering
            lead_data = process_apify_post(normalized_post_data, config)

            if not lead_data:
                # Post didn't pass filters - mark as scraped but don't save lead
                mark_activity_scraped("linkedin", activity_id)
                print(
                    f"  [{i+1}/{len(apify_posts)}] Skipped: {post_data.get('author', {}).get('name', 'Unknown')} (filtered out)"
                )
                continue

            # Display processing info
            print(
                f"  [{i+1}/{len(apify_posts)}] Processing: {lead_data['author_name']}"
            )
            print(f"    Matched keywords: {lead_data['matched_keywords']}")
            print(f"    Matched roles: {lead_data['matched_roles']}")
            print(f"    Matched categories: {lead_data['matched_categories']}")

            # Save lead to database
            save_lead("linkedin", lead_data["activity_id"], lead_data)

            # Mark as successfully scraped
            mark_activity_scraped("linkedin", activity_id)
            processed_count += 1
            print(
                f"    Saved lead: {lead_data['author_name']} at {lead_data['company'] or 'Unknown Company'}"
            )

        except Exception as e:
            logger.error(f"Failed to process post {i+1}: {e}")

            # Track failed activity_id for rollback if needed
            if activity_id:
                failed_activity_ids.append(activity_id)

            print(f"  [{i+1}/{len(apify_posts)}] ERROR processing post: {e}")
            continue

    # Report results
    print(f"\nSuccessfully processed {processed_count} posts from Apify data")

    if failed_activity_ids:
        print(f"WARNING: {len(failed_activity_ids)} posts failed processing")

    return processed_count


def detect_matched_filters(
    post_content: str, author_title: str, config: Dict
) -> Dict[str, List[str]]:
    """Detect which keywords, roles, and categories matched in the post"""
    post_lower = post_content.lower()
    title_lower = author_title.lower() if author_title else ""

    matched_keywords = []
    matched_roles = []
    matched_categories = []

    # Check PR keywords
    for keyword in config.get("keywords", []):
        # Strip quotes from keyword for matching (defensive for any quoted keywords)
        clean_keyword = keyword.strip('"').lower()
        if clean_keyword in post_lower:
            matched_keywords.append(keyword)

    # Check role keywords
    for role in config.get("job_titles", []):
        if role.lower() in title_lower or role.lower() in post_lower:
            matched_roles.append(role)

    # Check CPG categories
    for category in config.get("industries", []):
        if category.lower() in title_lower or category.lower() in post_lower:
            matched_categories.append(category)

    return {
        "matched_keywords": matched_keywords,
        "matched_roles": matched_roles,
        "matched_categories": matched_categories,
    }


def main():
    print("LinkedIn PR Lead Scraper")
    print("=" * 60)

    try:
        init_database()
        config = load_config()
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
        return

    # Use Apify to get LinkedIn posts data
    apify_posts = get_apify_linkedin_posts(config)

    if not apify_posts:
        print(
            "No results found. Check your API configuration or try different keywords."
        )
        return

    # Process posts with improved error handling and rollback
    processed_count = process_apify_posts_batch(apify_posts, config)


if __name__ == "__main__":
    main()
