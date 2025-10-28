"""Generate LinkedIn search URLs from keywords for Google Sheets"""
import json
import urllib.parse
import csv

# Load keywords from config
with open("config.json", "r") as f:
    config = json.load(f)

keywords = config.get("keywords", [])

# Generate search URLs
search_urls = []
for keyword in keywords:
    encoded = urllib.parse.quote_plus(keyword)
    search_url = f"https://www.linkedin.com/search/results/content/?keywords={encoded}&sortBy=date_posted"
    search_urls.append({"Search URL": search_url, "Keyword": keyword})

# Write to CSV
output_file = "linkedin_search_urls.csv"
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Search URL", "Keyword"])
    writer.writeheader()
    writer.writerows(search_urls)

print(f"✓ Generated {len(search_urls)} search URLs")
print(f"✓ Saved to {output_file}")
print(f"\nUpload this CSV to Google Sheets and use that sheet URL in your PhantomBuster agent config.")
print(f"\nFirst few URLs:")
for i, row in enumerate(search_urls[:3]):
    print(f"{i+1}. {row['Keyword']}: {row['Search URL'][:80]}...")
