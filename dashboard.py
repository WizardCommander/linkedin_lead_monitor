import streamlit as st
import json
import html
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from database import (
    init_database,
    get_leads_filtered,
    get_lead_count,
    get_leads_today_count,
    dismiss_lead,
)
from monitor import ScraperMonitor

# Page config
st.set_page_config(page_title="PR Lead Bot Dashboard", page_icon="🤖", layout="wide")

# Initialize
if "monitor" not in st.session_state:
    init_database()
    st.session_state.monitor = ScraperMonitor()

monitor = st.session_state.monitor

# Custom CSS
st.markdown(
    """
<style>
    /* Hide Streamlit UI elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Lead card styling - containerized with blue left border */
    .lead-card {
        background-color: transparent;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 24px;
        border: 2px solid #3b82f6;
        border-left: 6px solid #3b82f6;
        transition: all 0.2s ease;
    }

    .lead-card:hover {
        border-color: #2563eb;
        border-left-color: #2563eb;
        transform: translateX(4px);
    }

    .lead-header {
        font-weight: 600;
        font-size: 1.15em;
        margin-bottom: 6px;
        color: #ffffff;
    }

    .lead-meta {
        color: #a0aec0;
        font-size: 0.9em;
        margin-bottom: 12px;
    }

    .lead-content {
        color: #e2e8f0;
        line-height: 1.6;
        margin-bottom: 12px;
    }

    /* Filter pill styling with distinct colors */
    .matched-pill {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 16px;
        margin-right: 6px;
        margin-bottom: 6px;
        font-size: 0.85em;
        font-weight: 500;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    }

    .pill-keyword {
        background-color: #ff9800;
        color: #ffffff;
    }

    .pill-role {
        background-color: #9c27b0;
        color: #ffffff;
    }

    .pill-category {
        background-color: #00897b;
        color: #ffffff;
    }

    /* Section headers */
    .section-header {
        background: linear-gradient(90deg, #f5f5f5 0%, #ffffff 100%);
        padding: 12px 16px;
        border-radius: 6px;
        margin-bottom: 16px;
        border-left: 3px solid #1976d2;
    }

    /* Button styling */
    .stButton > button[kind="primary"] {
        background-color: #1976d2;
        color: white;
        border: none;
    }

    .stButton > button[kind="secondary"] {
        background-color: transparent;
        color: #d32f2f;
        border: 1px solid #d32f2f;
    }

    /* Improve spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* Timestamp styling */
    .timestamp {
        color: #9e9e9e;
        font-size: 0.85em;
        font-style: italic;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Title
st.title("🤖 PR Lead Bot Dashboard")
st.markdown("Monitor X, BlueSky & Reddit for CPG clients seeking PR agencies")

# Configuration Panel (Sidebar)
st.sidebar.header("⚙️ Bot Configuration")


# Load config
def load_config() -> Dict[str, Any]:
    """Load configuration from config.json"""
    with open("config.json", "r") as f:
        return json.load(f)


def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to config.json"""
    with open("config.json", "w") as f:
        json.dump(config, f, indent=2)


config = load_config()

# PR Intent Keywords
st.sidebar.subheader("🔍 PR Intent Keywords")
pr_keywords = config.get("keywords", [])
new_keyword = st.sidebar.text_input("Add keyword:", key="new_pr_keyword")
if st.sidebar.button("➕ Add", key="add_pr_keyword") and new_keyword:
    if new_keyword not in pr_keywords:
        pr_keywords.append(new_keyword)
        config["keywords"] = pr_keywords
        save_config(config)
        st.rerun()

# Display current keywords
for i, keyword in enumerate(pr_keywords):
    col1, col2 = st.sidebar.columns([4, 1])
    col1.write(f"• {keyword}")
    if col2.button("❌", key=f"del_pr_{i}"):
        pr_keywords.remove(keyword)
        config["keywords"] = pr_keywords
        save_config(config)
        st.rerun()

# Role Keywords
st.sidebar.subheader("👔 Role Keywords")
role_keywords = config.get("job_titles", [])
new_role = st.sidebar.text_input("Add role:", key="new_role")
if st.sidebar.button("➕ Add", key="add_role") and new_role:
    if new_role not in role_keywords:
        role_keywords.append(new_role)
        config["job_titles"] = role_keywords
        save_config(config)
        st.rerun()

for i, role in enumerate(role_keywords):
    col1, col2 = st.sidebar.columns([4, 1])
    col1.write(f"• {role}")
    if col2.button("❌", key=f"del_role_{i}"):
        role_keywords.remove(role)
        config["job_titles"] = role_keywords
        save_config(config)
        st.rerun()

# CPG Categories
st.sidebar.subheader("🏭 CPG Categories")
cpg_categories = config.get("industries", [])
new_category = st.sidebar.text_input("Add category:", key="new_category")
if st.sidebar.button("➕ Add", key="add_category") and new_category:
    if new_category not in cpg_categories:
        cpg_categories.append(new_category)
        config["industries"] = cpg_categories
        save_config(config)
        st.rerun()

for i, category in enumerate(cpg_categories):
    col1, col2 = st.sidebar.columns([4, 1])
    col1.write(f"• {category}")
    if col2.button("❌", key=f"del_cat_{i}"):
        cpg_categories.remove(category)
        config["industries"] = cpg_categories
        save_config(config)
        st.rerun()

# Monitoring Status
st.header("📊 Monitoring Status")

status = monitor.get_monitoring_status()

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_leads = get_lead_count()
    st.metric("Total Leads", total_leads)

with col2:
    today_leads = get_leads_today_count()
    st.metric("Today", today_leads)

with col3:
    platform_badge = "LinkedIn"
    st.metric("Platform", platform_badge)

with col4:
    status_text = "🟢 Active" if status["active"] else "⚫ Stopped"
    st.metric("Status", status_text)

# Control buttons
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("▶️ Start Monitoring", type="primary", use_container_width=True):
        monitor.start_monitoring()
        st.success("Monitoring started!")
        time.sleep(1)
        st.rerun()

with col2:
    if st.button("⏸️ Stop Monitoring", use_container_width=True):
        monitor.stop_monitoring()
        st.warning("Monitoring stopped")
        time.sleep(1)
        st.rerun()

with col3:
    if st.button("🔄 Run Once", use_container_width=True):
        with st.spinner("Running scraper..."):
            monitor.run_once()
        st.success("Scrape completed!")
        time.sleep(2)
        st.rerun()

# Recent Leads
st.header("📋 Recent Leads")

# Filters
col1, col2, col3, col4 = st.columns(4)

with col1:
    filter_category = st.selectbox(
        "Filter by category", ["All"] + cpg_categories, key="filter_category"
    )

with col2:
    filter_role = st.selectbox(
        "Filter by role", ["All"] + role_keywords, key="filter_role"
    )

with col3:
    filter_keyword = st.selectbox(
        "Filter by keyword", ["All"] + pr_keywords, key="filter_keyword"
    )

with col4:
    search_text = st.text_input("🔍 Search posts", key="search_text")

# Get filtered leads
filter_params = {"platform": "linkedin", "include_dismissed": False, "limit": 50}

if filter_category and filter_category != "All":
    filter_params["category"] = filter_category

if filter_role and filter_role != "All":
    filter_params["role"] = filter_role

if filter_keyword and filter_keyword != "All":
    filter_params["keyword"] = filter_keyword

if search_text:
    filter_params["search_text"] = search_text

leads = get_leads_filtered(**filter_params)

# Display leads
if not leads:
    st.info("No leads found. Try adjusting filters or run the scraper.")
else:
    st.write(f"Showing {len(leads)} lead(s)")

    for lead in leads:
        # Parse matched filters
        matched_keywords = json.loads(lead.get("matched_keywords") or "[]")
        matched_roles = json.loads(lead.get("matched_roles") or "[]")
        matched_categories = json.loads(lead.get("matched_categories") or "[]")

        # Create card with custom styling
        with st.container():
            # Build entire lead card as HTML
            author_name = html.escape(lead.get("author_name") or "Unknown")
            author_title_raw = lead.get("author_title") or ""
            author_title = html.escape(author_title_raw.strip())

            if author_title:
                author_display = f'<div class="lead-header">{author_name}</div><div class="lead-meta">{author_title}</div>'
            else:
                author_display = f'<div class="lead-header">{author_name}</div><div class="lead-meta">No title available</div>'

            # Post content preview (HTML escaped for XSS protection)
            content = lead.get("post_content") or ""
            preview = content[:300] + "..." if len(content) > 300 else content
            preview_escaped = html.escape(preview)

            # Matched filters with vibrant pills (HTML escaped for XSS protection)
            matches_html = ""
            if matched_keywords or matched_roles or matched_categories:
                matches_html = "<div style='margin-top: 12px;'>"
                for kw in matched_keywords:
                    kw_escaped = html.escape(str(kw))
                    matches_html += f'<span class="matched-pill pill-keyword">🔍 {kw_escaped}</span>'
                for role in matched_roles:
                    role_escaped = html.escape(str(role))
                    matches_html += (
                        f'<span class="matched-pill pill-role">👔 {role_escaped}</span>'
                    )
                for cat in matched_categories:
                    cat_escaped = html.escape(str(cat))
                    matches_html += f'<span class="matched-pill pill-category">🏭 {cat_escaped}</span>'
                matches_html += "</div>"

            # Timestamp
            timestamp_html = ""
            scraped_at = lead.get("scraped_at", "")
            if scraped_at:
                try:
                    dt = datetime.fromisoformat(scraped_at)
                    time_ago = datetime.now() - dt
                    if time_ago.days > 0:
                        time_str = f"{time_ago.days}d ago"
                    elif time_ago.seconds // 3600 > 0:
                        time_str = f"{time_ago.seconds // 3600}h ago"
                    else:
                        time_str = f"{time_ago.seconds // 60}m ago"
                    timestamp_html = f'<div class="timestamp">Posted {time_str}</div>'
                except Exception:
                    timestamp_html = (
                        f'<div class="timestamp">Posted at {scraped_at}</div>'
                    )

            # Render as two columns with lead card only wrapping left content
            col1, col2 = st.columns([5, 1])

            with col1:
                full_card_html = f"""
                <div class="lead-card">
                    {author_display}
                    <div class="lead-content">{preview_escaped}</div>
                    {matches_html}
                    {timestamp_html}
                </div>
                """
                st.markdown(full_card_html, unsafe_allow_html=True)

            with col2:
                # Actions with improved styling
                if st.button(
                    "👁️ Preview",
                    key=f"preview_{lead['id']}",
                    type="primary",
                    use_container_width=True,
                ):
                    st.write(f"[Open LinkedIn Post]({lead.get('post_url', '#')})")

                if st.button(
                    "🗑️ Dismiss", key=f"dismiss_{lead['id']}", use_container_width=True
                ):
                    dismiss_lead(lead["id"])
                    st.rerun()

            st.divider()

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Auto-refresh every 30 seconds
if status["active"]:
    time.sleep(30)
    st.rerun()
