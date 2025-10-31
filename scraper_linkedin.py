import json
import re
import os
import logging
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI
from database import (
    init_database,
    save_lead,
    save_activity_id,
    mark_activity_scraped,
    save_processed_container,
    is_container_processed,
    get_processed_containers,
)
from utils import extract_budget_mention
from phantombuster_client import PhantomBusterClient
from phantombuster_parser import parse_phantombuster_output

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
    """Validate keyword format

    Note: Quotes are no longer required - they caused 0 results.
    LinkedIn search works better without exact phrase matching.

    Args:
        keywords: List of keyword strings

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

        # Check for reasonable length
        if len(keyword) < 3:
            errors.append(
                f"Keyword {i+1} '{keyword}' is too short (minimum 3 characters)"
            )

        if len(keyword) > 100:
            errors.append(
                f"Keyword {i+1} '{keyword}' is too long (maximum 100 characters)"
            )

        # Check for empty keyword
        if not keyword.strip():
            errors.append(f"Keyword {i+1} cannot be empty")

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
    required_sections = ["keywords", "job_titles", "industries"]
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

    return errors


def load_config():
    """Load configuration from .env and config.json"""
    config = {}

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


def filter_posts_by_date(posts: List[Dict], hours: int) -> List[Dict]:
    """Filter posts to only include those posted within the last N hours

    Args:
        posts: List of post dicts with 'timestamp' or 'created_at' field
        hours: Number of hours to look back from now

    Returns:
        Filtered list of posts within the time range
    """
    from datetime import datetime, timedelta

    if not hours or hours <= 0:
        # No filtering, return all posts
        return posts

    cutoff_time = datetime.now() - timedelta(hours=hours)
    filtered_posts = []

    for post in posts:
        # Try multiple timestamp field names
        timestamp_str = post.get("timestamp") or post.get("created_at") or post.get("posted_at")

        if not timestamp_str:
            # No timestamp, skip this post
            logger.warning(f"Post missing timestamp: {post.get('activity_id', 'unknown')}")
            continue

        try:
            # Parse ISO format timestamp
            post_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

            # Check if within range
            if post_time >= cutoff_time:
                filtered_posts.append(post)

        except (ValueError, AttributeError) as e:
            logger.warning(f"Invalid timestamp format: {timestamp_str}")
            continue

    logger.info(f"Date filter: {len(posts)} posts -> {len(filtered_posts)} posts within last {hours}h")
    return filtered_posts


def get_phantombuster_linkedin_posts(config: Dict, date_range_hours: Optional[int] = None) -> List[Dict]:
    """Get LinkedIn posts data using PhantomBuster API

    Supports two modes:
    1. Fresh scrape: Launches agent for new keyword searches
    2. Historical fetch: Retrieves cached results from previous scrapes

    When PhantomBuster returns "already retrieved", fetches from existing
    containers instead of failing.

    Args:
        config: Configuration dictionary with PhantomBuster settings
        date_range_hours: If specified, only return posts within this many hours from now

    Returns:
        List of post data dictionaries with URLs, content, author info, etc.
    """
    import urllib.parse

    pb_config = config.get("phantombuster", {})

    if not pb_config.get("enabled"):
        logger.warning("PhantomBuster is not enabled in config")
        return []

    agent_id = pb_config.get("agent_id")

    if not agent_id:
        raise ConfigurationError("PhantomBuster agent_id not configured")

    keywords = config.get("keywords", [])
    if not keywords:
        logger.warning("No keywords configured")
        return []

    print(f"Searching LinkedIn via PhantomBuster...")
    print(f"  Agent ID: {agent_id}")
    print(f"  Keywords: {len(keywords)}")
    if date_range_hours:
        print(f"  Date filter: Last {date_range_hours} hours")

    try:
        # Initialize PhantomBuster client
        client = PhantomBusterClient()

        all_posts = []

        # Launch agent for each keyword search URL
        for i, keyword in enumerate(keywords):
            print(f"\n  [{i+1}/{len(keywords)}] Searching: '{keyword}'")

            # Try to launch agent with keyword (agent is in Keywords mode)
            print(f"    Launching agent...")
            launch_result = client.launch_agent(agent_id, keyword)

            # Handle two modes: fresh scrape or cached results
            if launch_result["cached"]:
                # PhantomBuster says "already retrieved" - fetch from historical containers
                print(f"    ⚠️  {launch_result['message']}")
                print(f"    Fetching from historical containers...")

                # Get all containers for this agent
                containers = client.get_all_containers(agent_id, limit=10)

                if not containers:
                    print(f"    No historical containers found")
                    continue

                # Use the most recent unprocessed container
                # (or most recent overall if all are processed)
                container_to_fetch = None
                for container in containers:
                    container_id = str(container.get("id"))
                    if not is_container_processed(container_id):
                        container_to_fetch = container
                        break

                # If all containers are processed, use the most recent one anyway
                # (user might have changed date_range_hours filter)
                if not container_to_fetch and containers:
                    container_to_fetch = containers[0]

                if not container_to_fetch:
                    print(f"    No suitable container found")
                    continue

                container_id = str(container_to_fetch.get("id"))
                print(f"    Using container: {container_id}")

                # Fetch output from this container
                print(f"    Fetching results...")
                raw_output = client.fetch_output_by_container_id(container_id)

                # Save this container as processed
                save_processed_container(container_id, agent_id, keyword, len(raw_output) if isinstance(raw_output, list) else 0)

            else:
                # Fresh scrape - agent launched successfully
                container_id = launch_result["container_id"]
                print(f"    Container ID: {container_id}")

                # Wait for completion
                poll_interval = pb_config.get("poll_interval", 30)
                timeout = pb_config.get("timeout", 900)

                print(f"    Waiting for agent to complete (timeout: {timeout}s)...")
                status = client.wait_for_completion(
                    agent_id, container_id, poll_interval=poll_interval, timeout=timeout
                )
                print(f"    Agent completed successfully")

                # Fetch output
                print(f"    Fetching results...")
                raw_output = client.fetch_output(agent_id, container_id)

                # Save this container as processed
                save_processed_container(container_id, agent_id, keyword, len(raw_output) if isinstance(raw_output, list) else 0)

            print(f"    Raw output type: {type(raw_output)}, length: {len(raw_output) if isinstance(raw_output, list) else 'N/A'}")

            # Debug: show first item structure
            if raw_output and isinstance(raw_output, list) and len(raw_output) > 0:
                print(f"    First item keys: {list(raw_output[0].keys())}")
                print(f"    First item sample: {str(raw_output[0])[:200]}")

            # Parse and normalize output
            normalized_posts = parse_phantombuster_output(raw_output)

            print(f"    Found {len(normalized_posts)} valid posts after parsing")

            # Add search keyword to each post for tracking
            for post in normalized_posts:
                post["search_input"] = keyword

            all_posts.extend(normalized_posts)

        # Remove duplicates by activity_id
        seen_activity_ids = set()
        unique_posts = []
        for post in all_posts:
            activity_id = post.get("activity_id")
            if activity_id and activity_id not in seen_activity_ids:
                seen_activity_ids.add(activity_id)
                unique_posts.append(post)
            elif not activity_id:
                # If no activity_id, keep it anyway
                unique_posts.append(post)

        # Apply date filtering if specified
        if date_range_hours:
            unique_posts = filter_posts_by_date(unique_posts, date_range_hours)

        print(f"\nPhantomBuster search complete:")
        print(f"  Total posts fetched: {len(all_posts)}")
        print(f"  Unique posts (after dedup): {len(unique_posts)}")
        if date_range_hours:
            print(f"  After date filter ({date_range_hours}h): {len(unique_posts)}")

        return unique_posts

    except Exception as e:
        print(f"  ERROR: PhantomBuster failed: {e}")
        logger.error(f"PhantomBuster error: {e}")
        raise


def get_linkedin_posts(config: Dict, date_range_hours: Optional[int] = None) -> List[Dict]:
    """Get LinkedIn posts using PhantomBuster

    Args:
        config: Configuration dictionary
        date_range_hours: If specified, only return posts within this many hours from now

    Returns:
        List of post data dictionaries
    """
    return get_phantombuster_linkedin_posts(config, date_range_hours)


def extract_activity_id(url: str) -> Optional[str]:
    """Extract activity ID from LinkedIn post URL

    Handles LinkedIn URL format: linkedin.com/posts/username-7380301291354263553-tCfn
    Activity IDs are always 19 digits.

    Args:
        url: LinkedIn post URL

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


def validate_post(post_data: dict) -> bool:
    """Validate that post data has required fields

    Args:
        post_data: Raw post data

    Returns:
        True if post has all required fields, False otherwise
    """
    if not isinstance(post_data, dict):
        return False

    required_fields = ["activity_id", "post_url", "text", "author_name"]
    return all(
        field in post_data and post_data[field] is not None for field in required_fields
    )


def extract_lead_data(post_data: dict) -> dict:
    """Extract and normalize lead data from post

    Args:
        post_data: Validated post data (already normalized from PhantomBuster)

    Returns:
        Normalized lead data dictionary
    """
    # Preserve any stats object from PhantomBuster, or build from individual fields
    stats = post_data.get("stats", {})
    if not isinstance(stats, dict):
        stats = {}

    # Ensure we have at minimum likes and comments (even if 0)
    if "likes" not in stats:
        stats["likes"] = post_data.get("likes", 0)
    if "comments" not in stats:
        stats["comments"] = post_data.get("comments", 0)

    return {
        "activity_id": post_data.get("activity_id", ""),
        "url": post_data.get("post_url", ""),
        "post_url": post_data.get("post_url", ""),
        "author_name": post_data.get("author_name", "Unknown"),
        "author_title": post_data.get("author_title", ""),
        "post_content": post_data.get("text", ""),
        "posted_at": post_data.get("timestamp", ""),
        "hashtags": post_data.get("hashtags", []),
        "stats": stats,  # Preserve all stats from PhantomBuster
        "search_input": post_data.get("search_input", ""),
    }


def passes_pr_content_filter(post_data: dict, config: dict) -> bool:
    """Very loose pre-filter - just check if post mentions PR-related content

    This is intentionally permissive to let GPT do the heavy lifting.
    We only reject posts that clearly have nothing to do with PR.

    Args:
        post_data: Post data
        config: Configuration dict, can override pr_indicators via config.get("pr_indicators")

    Returns:
        True if post mentions PR-related content, False otherwise
    """
    post_content = post_data.get("text", "").lower()
    activity_id = post_data.get("activity_id", "unknown")

    # Default PR indicators - can be overridden via config
    # Use space-bounded " pr " to avoid false matches like "april", "approval"
    default_pr_indicators = [
        " pr ",
        "pr agency",
        "pr firm",
        "pr partner",
        "pr support",
        "pr help",
        "pr consultant",
        "pr freelancer",
        "public relations",
        "publicist",
        "pr recommendations",
        "pr recs",
    ]

    pr_indicators = config.get("pr_indicators", default_pr_indicators)

    for indicator in pr_indicators:
        if indicator in post_content:
            return True

    # If no matches, log for debugging
    logger.info(f"FILTERED OUT {activity_id}: '{post_content[:150]}...'")
    logger.info(f"  Search term: {post_data.get('search_input', 'unknown')}")

    return False


# Global circuit breaker state
GPT_FAILURE_COUNT = 0
GPT_CIRCUIT_BREAKER_THRESHOLD = 5
GPT_CIRCUIT_BREAKER_ACTIVE = False

# Cost monitoring
GPT_DAILY_CALL_COUNT = 0
GPT_DAILY_CALL_LIMIT = 1000  # Configurable daily limit
GPT_LAST_RESET_DATE = None

# Cost tracking per run
GPT_COST_THIS_RUN = 0.0
GPT_MAX_COST_PER_RUN = 5.00  # Safety limit: max $5 per scrape run

# Model pricing (per 1M tokens)
GPT_MODEL_COSTS = {
    "gpt-4o": {"input": 2.50 / 1_000_000, "output": 10.00 / 1_000_000},
    "gpt-4o-mini": {"input": 0.15 / 1_000_000, "output": 0.60 / 1_000_000},
}


def track_gpt_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Track cost of GPT API call and enforce budget limits

    Args:
        model: Model name (e.g., "gpt-4o")
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Cost of this call in dollars

    Raises:
        Exception: If cost limit exceeded
    """
    global GPT_COST_THIS_RUN

    if model not in GPT_MODEL_COSTS:
        logger.warning(f"Unknown model {model}, cannot track cost")
        return 0.0

    costs = GPT_MODEL_COSTS[model]
    call_cost = (input_tokens * costs["input"]) + (output_tokens * costs["output"])
    GPT_COST_THIS_RUN += call_cost

    logger.info(
        f"GPT call cost: ${call_cost:.4f} (input: {input_tokens}, output: {output_tokens})"
    )
    logger.info(
        f"Total cost this run: ${GPT_COST_THIS_RUN:.2f} / ${GPT_MAX_COST_PER_RUN:.2f}"
    )

    if GPT_COST_THIS_RUN > GPT_MAX_COST_PER_RUN:
        raise Exception(
            f"Cost limit exceeded: ${GPT_COST_THIS_RUN:.2f} > ${GPT_MAX_COST_PER_RUN:.2f}. "
            f"Stopping to prevent runaway costs."
        )

    return call_cost


def reset_run_cost():
    """Reset the cost counter for this run"""
    global GPT_COST_THIS_RUN
    GPT_COST_THIS_RUN = 0.0
    logger.info("Reset GPT cost counter for new run")


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
    post_content: str,
    author_name: str,
    author_title: str,
    post_url: str,
    config: dict = None,
) -> dict:
    """Use GPT to analyze if this is a genuine PR lead for BDPR

    Args:
        post_content: LinkedIn post text
        author_name: Author's name
        author_title: Author's job title
        post_url: URL to the post
        config: Configuration dict with GPT settings
    """
    if config is None:
        config = {}

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

    prompt = f"""Analyze this LinkedIn post to determine if the author is seeking external PR help.

POST CONTENT: "{post_content}"
AUTHOR: {author_name}
AUTHOR TITLE: {author_title}

ACCEPTANCE CRITERIA - Must meet ALL required criteria:

1. DIRECT INTENT TO HIRE (REQUIRED):
   ✅ ACCEPT if author is seeking/looking for/needs external PR help:
      - "looking for PR agency/firm/partner/support/freelancer"
      - "seeking PR help", "need PR assistance", "PR recommendations"
      - "hiring PR", "RFP for PR", "who should I work with for PR"
   ✅ Accept variations in phrasing: "PR support", "PR partner", "boutique PR", "PR consultant"
   ❌ REJECT if just mentioning PR without seeking help
   ❌ REJECT educational posts ABOUT PR (explaining what PR is)
   ❌ REJECT posts about launches/funding WITHOUT requesting PR help

2. EXTERNAL HELP (REQUIRED):
   ✅ ACCEPT: Seeking agency, boutique firm, freelancer, consultant, PR partner, publicist
   ❌ REJECT: Hiring in-house employees ("join our team", job postings with "apply now")
   ⚠️  If unclear, lean toward accepting if other signals are strong

3. NOT A SERVICE PROVIDER (REQUIRED):
   ❌ REJECT: PR agencies/consultants OFFERING their services
   ❌ REJECT: "We help companies with PR", "Our agency offers"
   ❌ REJECT: Author title contains "PR Agency", "PR Consultant" (they're providers, not seekers)
   ✅ ACCEPT: Decision makers at brands (Founder, CMO, Brand Manager, etc.)

4. DECISION MAKER (PREFERRED):
   ✅ IDEAL roles: {target_roles}
   ✅ Also accept: CEO, Founder, Co-Founder, other C-level
   ⚠️  Accept with lower confidence: Other business roles

5. RELEVANT INDUSTRY (PREFERRED):
   ✅ IDEAL: {target_industries}
   ✅ Also accept: Other CPG, DTC, consumer brands
   ⚠️  Accept with lower confidence: Other industries

IMPORTANT: Be BALANCED - accept genuine leads even if phrasing varies.
Example of a post to ACCEPT: "Seeking PR Support: Boutique Agency or Freelancer. [Brand] is looking for a PR partner to help share our story. Looking for someone who understands wellness and can translate our values-driven brand into meaningful media moments."

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

            # Get model from config, default to gpt-4o-mini for safety
            model = config.get("gpt", {}).get("model", "gpt-4o-mini")

            response = client.chat.completions.create(
                model=model,
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

            # Track cost of this API call
            usage = response.usage
            track_gpt_cost(model, usage.prompt_tokens, usage.completion_tokens)

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
    if not passes_pr_content_filter(post_data, config):
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


def process_post(post_data: dict, config: dict) -> Optional[dict]:
    """Process single post into lead data with filtering

    Args:
        post_data: Raw post data
        config: Configuration dictionary

    Returns:
        Lead data dict if post passes filters, None otherwise
    """
    # Validate required fields
    if not validate_post(post_data):
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
    print(
        f"      Post Content: {lead_data.get('post_content', '')[:150]}{'...' if len(lead_data.get('post_content', '')) > 150 else ''}"
    )

    # Apply GPT analysis for qualified posts
    gpt_analysis = analyze_lead_with_gpt(
        lead_data["post_content"],
        lead_data["author_name"],
        lead_data["author_title"],
        lead_data["url"],
        config,
    )

    # Show detailed GPT analysis results
    print(f"    [GPT RESULT] Analysis complete:")
    print(
        f"      Decision: {'✅ ACCEPT' if gpt_analysis.get('is_genuine_lead', False) else '❌ REJECT'}"
    )
    print(f"      Confidence: {gpt_analysis.get('confidence_score', 'N/A')}%")
    print(f"      Lead Quality: {gpt_analysis.get('lead_quality', 'N/A')}")
    print(f"      Hiring Type: {gpt_analysis.get('hiring_type', 'N/A')}")
    print(f"      Industry Match: {gpt_analysis.get('industry_match', 'N/A')}")
    print(f"      Target Role Match: {gpt_analysis.get('target_role_match', 'N/A')}")
    print(f"      Reasoning: {gpt_analysis.get('reasoning', 'No reason provided')}")

    urgency = gpt_analysis.get("urgency_indicators", [])
    if urgency:
        print(f"      Urgency Indicators: {', '.join(urgency)}")

    red_flags = gpt_analysis.get("red_flags", [])
    if red_flags:
        print(f"      Red Flags: {', '.join(red_flags)}")

    # Filter out non-genuine leads based on GPT analysis
    if not gpt_analysis.get("is_genuine_lead", False):
        activity_id = lead_data.get("activity_id", "unknown")
        print(f"    [FILTERED] Post rejected by GPT analysis")
        logger.info(
            f"GPT FILTERED OUT {activity_id}: {gpt_analysis.get('reasoning', 'No reason provided')}"
        )
        return None

    # Enrich with computed fields and GPT analysis results
    enriched_data = enrich_lead_data(lead_data, config)

    # Show successful GPT qualification
    print(f"    [QUALIFIED] ✅ Lead accepted for database save")
    print(f"      Final Decision: GENUINE LEAD")

    # Add GPT analysis results to the lead data
    # Store complete GPT analysis as JSON in raw_data field
    enriched_data["raw_data"] = json.dumps(gpt_analysis)

    return enriched_data


def process_posts_batch(posts: List[Dict], config: Dict) -> int:
    """Process batch of posts with error handling and rollback

    Args:
        posts: List of post data
        config: Configuration dictionary

    Returns:
        Number of successfully processed posts
    """
    print(f"Processing {len(posts)} posts from PhantomBuster data")

    processed_count = 0
    failed_activity_ids = []

    for i, post_data in enumerate(posts):
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
            lead_data = process_post(normalized_post_data, config)

            if not lead_data:
                # Post didn't pass filters - mark as scraped but don't save lead
                mark_activity_scraped("linkedin", activity_id)
                print(
                    f"  [{i+1}/{len(posts)}] Skipped: {post_data.get('author_name', 'Unknown')} (filtered out)"
                )
                continue

            # Display processing info
            print(f"  [{i+1}/{len(posts)}] Processing: {lead_data['author_name']}")
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

            print(f"  [{i+1}/{len(posts)}] ERROR processing post: {e}")
            continue

    # Report results
    print(f"\nSuccessfully processed {processed_count} posts from PhantomBuster data")

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

    # Reset cost counter at start of run
    reset_run_cost()

    try:
        init_database()
        config = load_config()

        # Load max cost from config
        global GPT_MAX_COST_PER_RUN
        GPT_MAX_COST_PER_RUN = config.get("gpt", {}).get("max_cost_per_run", 5.0)

        # Show GPT settings
        gpt_model = config.get("gpt", {}).get("model", "gpt-4o-mini")
        print(f"GPT Model: {gpt_model}")
        print(f"Cost Limit: ${GPT_MAX_COST_PER_RUN:.2f} per run")
        print()
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
        return

    # Get date range filter from config (optional)
    date_range_hours = config.get("monitoring", {}).get("date_range_hours")

    # Get LinkedIn posts data (routes to PhantomBuster or Apify based on config)
    posts = get_linkedin_posts(config, date_range_hours=date_range_hours)

    if not posts:
        print(
            "No results found. Check your API configuration or try different keywords."
        )
        return

    # Process posts with improved error handling and rollback
    processed_count = process_posts_batch(posts, config)

    # Report final cost
    print(f"\n💰 Total GPT cost this run: ${GPT_COST_THIS_RUN:.2f}")


if __name__ == "__main__":
    main()
