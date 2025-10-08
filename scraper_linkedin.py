import json
import re
import time
import os
import logging
import requests
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, Page
from dotenv import load_dotenv
from database import init_database, save_lead, save_activity_id, mark_activity_scraped, get_unscraped_activity_ids
from utils import extract_budget_mention

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MAX_KEYWORDS_IN_QUERY = 15
MAX_JOB_TITLES_IN_QUERY = 7
MAX_INDUSTRIES_IN_QUERY = 7
SCRAPE_DELAY_SECONDS = 2
MIN_POST_CONTENT_LENGTH = 50
SKIP_FIRST_N_LINES = 2

class ConfigurationError(Exception):
    """Raised when configuration is missing or invalid"""
    pass

def load_config():
    """Load configuration from .env and config.json"""
    config = {}

    # Load API credentials from .env
    config["google_api_key"] = os.getenv("GOOGLE_SEARCH_API_KEY")
    config["google_cx_id"] = os.getenv("SEARCH_ENGINE_ID")

    if not config["google_api_key"] or not config["google_cx_id"]:
        raise ConfigurationError(
            "Missing Google API credentials in .env file. "
            "Add GOOGLE_SEARCH_API_KEY and SEARCH_ENGINE_ID to your .env file"
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

    return config

def build_mega_query(keywords: List[str], job_titles: List[str], industries: List[str], date_filter: Optional[str] = None) -> str:
    """Build a Google search mega-query combining keywords, titles, and industries"""
    keyword_part = " OR ".join([f'"{k}"' for k in keywords[:MAX_KEYWORDS_IN_QUERY]])
    title_part = " OR ".join([f'"{t}"' for t in job_titles[:MAX_JOB_TITLES_IN_QUERY]])
    industry_part = " OR ".join(industries[:MAX_INDUSTRIES_IN_QUERY])

    query = f'site:linkedin.com/posts ({keyword_part})'

    if job_titles:
        query += f' ({title_part})'

    if industries:
        query += f' ({industry_part})'

    if date_filter:
        query += f' {date_filter}'

    return query

def search_google_api(config: Dict, query: str, max_results: int = 10) -> List[str]:
    """Search using Google Custom Search API"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": config["google_api_key"],
        "cx": config["google_cx_id"],
        "q": query,
        "num": min(max_results, 10)
    }

    try:
        print(f"🔍 Querying Google API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        urls = []
        for item in data.get("items", []):
            link = item.get("link", "")
            if "linkedin.com/posts/" in link and "-activity-" in link:
                urls.append(link)

        return urls

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("❌ Rate limit exceeded. You've used your daily quota.")
        elif e.response.status_code == 400:
            print("❌ Bad request. Check your API key and CX ID.")
        else:
            print(f"❌ HTTP Error: {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error: {e}")
        return []

def extract_activity_id(url: str) -> Optional[str]:
    """Extract activity ID from LinkedIn URL"""
    match = re.search(r'activity-(\d+)', url)
    return match.group(1) if match else None

def detect_matched_filters(post_content: str, author_title: str, config: Dict) -> Dict[str, List[str]]:
    """Detect which keywords, roles, and categories matched in the post"""
    post_lower = post_content.lower()
    title_lower = author_title.lower() if author_title else ""

    matched_keywords = []
    matched_roles = []
    matched_categories = []

    # Check PR keywords
    for keyword in config.get("keywords", []):
        if keyword.lower() in post_lower:
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
        "matched_categories": matched_categories
    }

def scrape_linkedin_post(page: Page, url: str) -> Dict:
    """Scrape content from a LinkedIn share URL"""
    try:
        page.goto(url, timeout=20000)
        page.wait_for_timeout(2000)

        # Extract post content more precisely
        content = ""
        author_name = "Unknown"
        author_title = ""

        try:
            # Try to find the main post text content
            content_selectors = [
                ".feed-shared-update-v2__description",
                ".feed-shared-text",
                "[data-test-id='main-feed-activity-card__commentary']",
                ".update-components-text"
            ]

            for selector in content_selectors:
                try:
                    elem = page.locator(selector).first
                    content = elem.inner_text().strip()
                    if content:
                        break
                except Exception as e:
                    logger.debug(f"Selector '{selector}' failed: {e}")
                    continue

            # Fallback: get text from article but clean it
            if not content:
                article_text = page.locator("article").first.inner_text()
                # Extract content between author info and Like/Comment buttons
                lines = article_text.split('\n')
                content_lines = []
                skip_lines = False
                for line in lines:
                    line = line.strip()
                    if any(x in line.lower() for x in ['like', 'comment', 'share', 'report this']):
                        skip_lines = True
                    if not skip_lines and line and line not in ['', '7mo', '1w', '2w', '1mo']:
                        # Skip UI elements and timestamps
                        if not any(x in line for x in ['ago', 'Report', 'Reactions', 'Comments']):
                            content_lines.append(line)

                # The main post content usually starts after author title
                for i, line in enumerate(content_lines):
                    if i > SKIP_FIRST_N_LINES and len(line) > MIN_POST_CONTENT_LENGTH:
                        content = '\n'.join(content_lines[i:])
                        break

        except Exception as e:
            logger.debug(f"Fallback content extraction failed: {e}")
            content = ""

        # Extract author name and title
        try:
            author_name_elem = page.locator(".update-components-actor__name, .feed-shared-actor__name").first
            author_name = author_name_elem.inner_text().strip()
        except Exception as e:
            logger.debug(f"Primary author name extraction failed: {e}")
            try:
                author_name = page.locator("a[href*='/in/']").first.inner_text().strip()
            except Exception as e:
                logger.debug(f"Fallback author name extraction failed: {e}")
                pass

        try:
            author_title_elem = page.locator(".update-components-actor__description, .feed-shared-actor__description").first
            author_title = author_title_elem.inner_text().strip()
        except Exception as e:
            logger.debug(f"Author title extraction failed: {e}")
            pass

        hashtags = []
        try:
            hashtag_elements = page.locator("a[href*='/hashtag/']").all()
            hashtags = [el.inner_text().strip() for el in hashtag_elements]
        except Exception as e:
            logger.debug(f"Hashtag extraction failed: {e}")
            pass

        content = content.strip() if content else ""
        budget = extract_budget_mention(content)

        return {
            "success": True,
            "author_name": author_name,
            "author_title": author_title,
            "post_content": content,
            "hashtags": hashtags,
            "budget_mention": budget,
            "post_url": url
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "post_url": url
        }

def main():
    print("🔗 LinkedIn PR Lead Scraper")
    print("=" * 60)

    try:
        init_database()
        config = load_config()
    except ConfigurationError as e:
        print(f"❌ Configuration Error: {e}")
        return

    date_filter = config.get("monitoring", {}).get("date_filter", "after:yesterday")

    query = build_mega_query(
        config.get("keywords", []),
        config.get("job_titles", []),
        config.get("industries", []),
        date_filter=date_filter
    )

    print(f"\n📝 Search Query:\n{query[:200]}...\n")

    urls = search_google_api(config, query, max_results=10)
    print(f"✅ Found {len(urls)} LinkedIn post URLs\n")

    if not urls:
        print("No results found. Check your API configuration or try different keywords.")
        return

    new_activity_ids = []
    for url in urls:
        activity_id = extract_activity_id(url)
        if activity_id and save_activity_id("linkedin", activity_id):
            new_activity_ids.append((activity_id, url))

    print(f"📌 Discovered {len(new_activity_ids)} new activity IDs")

    unscraped = get_unscraped_activity_ids("linkedin")
    print(f"📄 {len(unscraped)} total unscraped posts in database\n")

    if not new_activity_ids:
        print("No new posts to scrape.")
        return

    print("🌐 Scraping post content with Playwright...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()

        scraped_count = 0
        for i, (activity_id, url) in enumerate(new_activity_ids):
            print(f"  [{i+1}/{len(new_activity_ids)}] {url}")

            result = scrape_linkedin_post(page, url)

            if result.get("success"):
                # Detect which filters matched
                matches = detect_matched_filters(
                    result.get("post_content", ""),
                    result.get("author_title", ""),
                    config
                )

                lead_data = {
                    "author_name": result.get("author_name"),
                    "author_title": result.get("author_title"),
                    "post_content": result.get("post_content"),
                    "post_url": result.get("post_url"),
                    "budget_mention": result.get("budget_mention"),
                    "matched_keywords": json.dumps(matches["matched_keywords"]),
                    "matched_roles": json.dumps(matches["matched_roles"]),
                    "matched_categories": json.dumps(matches["matched_categories"]),
                    "raw_data": json.dumps(result)
                }

                if save_lead("linkedin", activity_id, lead_data):
                    scraped_count += 1
                    mark_activity_scraped("linkedin", activity_id)
            else:
                print(f"    ⚠️  Error: {result.get('error')}")

            time.sleep(SCRAPE_DELAY_SECONDS)

        browser.close()

    print(f"\n✅ Successfully scraped {scraped_count} posts")
    print(f"💾 Data saved to pr_leads.db")

if __name__ == "__main__":
    main()
