import sqlite3
from database import DB_NAME, get_lead_count, get_recent_leads

def print_lead(lead, index):
    """Pretty print a single lead"""
    print(f"\n{'='*70}")
    print(f"Lead #{index}")
    print(f"{'='*70}")
    print(f"Platform:      {lead['platform'].upper()}")
    print(f"Author:        {lead['author_name'] or 'Unknown'}")
    if lead['author_handle']:
        print(f"Handle:        @{lead['author_handle']}")
    if lead['author_title']:
        print(f"Title/Bio:     {lead['author_title'][:80]}")
    if lead['company_name']:
        print(f"Company:       {lead['company_name']}")
    if lead['budget_mention']:
        print(f"💰 Budget:     {lead['budget_mention']}")
    print(f"Posted:        {lead['created_at'] or 'Unknown'}")
    print(f"Scraped:       {lead['scraped_at']}")
    print(f"\nContent Preview:")
    content = lead['post_content'] or ""
    print(f"  {content[:200]}{'...' if len(content) > 200 else ''}")
    print(f"\nURL: {lead['post_url']}")

def show_summary():
    """Show summary statistics"""
    total = get_lead_count()
    linkedin_count = get_lead_count("linkedin")
    bluesky_count = get_lead_count("bluesky")

    print("\n" + "="*70)
    print("📊 PR LEAD DATABASE SUMMARY")
    print("="*70)
    print(f"Total Leads:      {total}")
    print(f"LinkedIn:         {linkedin_count}")
    print(f"BlueSky:          {bluesky_count}")
    print("="*70)

def show_leads_with_budgets():
    """Show only leads that mention budgets"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM leads
        WHERE budget_mention IS NOT NULL
        ORDER BY scraped_at DESC
    """)

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("\n📭 No leads with budget mentions found.")
        return

    print(f"\n💰 Found {len(rows)} leads with budget mentions:")
    for i, row in enumerate(rows, 1):
        print_lead(dict(row), i)

def compare_platforms():
    """Compare lead quality between platforms"""
    print("\n" + "="*70)
    print("🔍 PLATFORM COMPARISON")
    print("="*70)

    linkedin_leads = get_recent_leads("linkedin", limit=50)
    bluesky_leads = get_recent_leads("bluesky", limit=50)

    linkedin_with_budget = sum(1 for l in linkedin_leads if l.get('budget_mention'))
    bluesky_with_budget = sum(1 for l in bluesky_leads if l.get('budget_mention'))

    print(f"\nLinkedIn:")
    print(f"  Total leads:     {len(linkedin_leads)}")
    print(f"  With budgets:    {linkedin_with_budget}")
    print(f"  Avg content len: {sum(len(l.get('post_content', '')) for l in linkedin_leads) // max(len(linkedin_leads), 1)} chars")

    print(f"\nBlueSky:")
    print(f"  Total leads:     {len(bluesky_leads)}")
    print(f"  With budgets:    {bluesky_with_budget}")
    print(f"  Avg content len: {sum(len(l.get('post_content', '')) for l in bluesky_leads) // max(len(bluesky_leads), 1)} chars")

def main():
    print("👀 PR Lead Viewer")
    show_summary()

    print("\n" + "="*70)
    print("OPTIONS:")
    print("1. View recent LinkedIn leads")
    print("2. View recent BlueSky leads")
    print("3. View all recent leads")
    print("4. View leads with budget mentions")
    print("5. Compare platforms")
    print("="*70)

    choice = input("\nEnter choice (1-5) or 'q' to quit: ").strip()

    if choice == "1":
        leads = get_recent_leads("linkedin", limit=10)
        print(f"\n📄 Showing {len(leads)} recent LinkedIn leads:")
        for i, lead in enumerate(leads, 1):
            print_lead(lead, i)

    elif choice == "2":
        leads = get_recent_leads("bluesky", limit=10)
        print(f"\n📄 Showing {len(leads)} recent BlueSky leads:")
        for i, lead in enumerate(leads, 1):
            print_lead(lead, i)

    elif choice == "3":
        leads = get_recent_leads(limit=10)
        print(f"\n📄 Showing {len(leads)} recent leads (all platforms):")
        for i, lead in enumerate(leads, 1):
            print_lead(lead, i)

    elif choice == "4":
        show_leads_with_budgets()

    elif choice == "5":
        compare_platforms()

    elif choice.lower() == "q":
        print("Goodbye!")
        return

    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
