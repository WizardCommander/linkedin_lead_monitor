# PR Lead Bot - LinkedIn Post Scraper

A bot that searches LinkedIn posts for marketing and brand leads in CPG companies looking for PR agencies.

## Features

- **Direct Post Search**: Searches LinkedIn posts by keywords using Apify
- **GPT Analysis**: Uses OpenAI GPT to analyze and filter relevant posts
- **Interactive Dashboard**: Streamlit web interface to review leads
- **Working Buttons**:
  - View Post - Opens LinkedIn post in new tab
  - Contact - Opens author's LinkedIn profile for messaging
  - Dismiss - Hides irrelevant leads

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Keys

Create a `.env` file with your API credentials:

```
APIFY_API_TOKEN=your_apify_token_here
OPENAI_API_KEY=your_openai_key_here
```

Get your API keys:
- **Apify**: https://console.apify.com/account/integrations
- **OpenAI**: https://platform.openai.com/api-keys

### 3. Customize Search (Optional)

Edit `config.json` to customize:
- **keywords**: PR-related phrases to search for
- **job_titles**: Target roles (CMO, VP Marketing, etc.)
- **industries**: Target industries (Beauty, CPG, etc.)
- **apify.results_per_keyword**: How many posts to get per keyword (1-50)

### 4. Run the Dashboard

```bash
streamlit run dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Usage

### Dashboard Controls

1. **▶️ Start Monitoring** - Enable automatic scheduled scraping (runs every 4 hours)
2. **⏸️ Stop Monitoring** - Disable scheduled scraping
3. **🔄 Run Once** - Manually run the scraper now

### Viewing Leads

- **Filters**: Filter by keyword, role, industry, or search text
- **👁️ View Post**: Opens the LinkedIn post in a new tab
- **💬 Contact**: Opens the author's LinkedIn profile
- **🗑️ Dismiss**: Removes the lead from your view

### How It Works

1. Searches LinkedIn for posts containing your keywords
2. Filters by job titles (only posts from CMOs, Marketing Directors, etc.)
3. GPT analyzes each post to determine if it's relevant
4. Saves qualified leads to SQLite database
5. Dashboard displays leads with matched keywords/roles/industries

## Configuration

### config.json Structure

```json
{
  "keywords": [
    "\"looking for a PR agency\"",
    "\"need a PR firm\"",
    ...
  ],
  "job_titles": [
    "CMO",
    "VP Marketing",
    ...
  ],
  "industries": [
    "Beauty",
    "CPG",
    ...
  ],
  "apify": {
    "results_per_keyword": 5,
    "date_filter": "past-week",
    "sort_type": "date_posted",
    "use_job_title_filter": true
  },
  "monitoring": {
    "active": false,
    "interval_hours": 4
  }
}
```

### Keyword Format

Keywords must be in quotes for exact phrase matching:
- ✅ `"looking for a PR agency"`
- ❌ `looking for a PR agency`

### Apify Settings

- **results_per_keyword**: 1-50 (higher = more posts but more expensive)
- **date_filter**: `past-24h`, `past-week`, `past-month`
- **sort_type**: `date_posted` or `relevance`

## Cost Estimates

- **Apify**: ~$0.05 per keyword search (~$1.25 for 25 keywords)
- **OpenAI GPT**: ~$0.01 per post analyzed (~$1.25 for 125 posts)
- **Total per run**: ~$2.50 (25 keywords × 5 posts each)
- **Monthly (6 runs/day)**: ~$450

Reduce costs by:
- Using fewer keywords
- Lowering `results_per_keyword`
- Changing `date_filter` to `past-24h`
- Running manually instead of scheduled

## Troubleshooting

### "Missing Apify API token"
Add `APIFY_API_TOKEN` to your `.env` file

### "config.json not found"
Make sure `config.json` exists in the same directory as `dashboard.py`

### No leads found
- Check that keywords are properly quoted
- Try broader keywords
- Increase `results_per_keyword`
- Change `date_filter` to `past-month`

### Buttons don't work
- Make sure you're using Streamlit 1.29.0 or higher
- The "View Post" and "Contact" buttons use `st.link_button()` which opens URLs in new tabs
- If buttons still don't work, check browser pop-up blocker settings

## Files

- `dashboard.py` - Streamlit web interface
- `scraper_linkedin.py` - LinkedIn scraper using Apify + GPT
- `database.py` - SQLite database layer
- `monitor.py` - Scheduling and orchestration
- `config.json` - Configuration (keywords, job titles, settings)
- `requirements.txt` - Python dependencies
- `.env` - API credentials (DO NOT COMMIT)

## Development

### Database Schema

The `leads` table stores:
- Post content and URL
- Author name, handle, title, company
- Matched keywords, roles, industries
- Timestamps and dismiss status

### Adding New Features

- **New keywords**: Edit `config.json` → `keywords`
- **New job titles**: Edit `config.json` → `job_titles`
- **New industries**: Edit `config.json` → `industries`
- **Change GPT prompt**: Edit `scraper_linkedin.py` → `analyze_lead_with_gpt()`

## License

MIT
