import json
import time
import logging
import schedule
from typing import Callable, Optional
from scraper_twitter import main as run_twitter_scraper
from database import init_database

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ScraperMonitor:
    """Manages scheduled scraping and monitoring state"""

    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.is_running = False
        self.last_run_status = "Not started"
        self.last_run_time = None
        self.next_run_time = None

    def load_config(self) -> dict:
        """Load configuration from JSON file"""
        with open(self.config_path, "r") as f:
            return json.load(f)

    def save_config(self, config: dict):
        """Save configuration to JSON file"""
        with open(self.config_path, "w") as f:
            json.dump(config, f, indent=2)

    def get_monitoring_status(self) -> dict:
        """Get current monitoring state"""
        config = self.load_config()
        return {
            "active": config.get("monitoring", {}).get("active", False),
            "interval_hours": config.get("monitoring", {}).get("interval_hours", 1),
            "is_running": self.is_running,
            "last_run_status": self.last_run_status,
            "last_run_time": self.last_run_time,
            "next_run_time": self.next_run_time,
        }

    def start_monitoring(self):
        """Enable monitoring in config"""
        config = self.load_config()
        if "monitoring" not in config:
            config["monitoring"] = {}
        config["monitoring"]["active"] = True
        self.save_config(config)
        logger.info("✅ Monitoring enabled")
        print("✅ Monitoring enabled")

    def stop_monitoring(self):
        """Disable monitoring in config"""
        config = self.load_config()
        if "monitoring" not in config:
            config["monitoring"] = {}
        config["monitoring"]["active"] = False
        self.save_config(config)
        logger.info("⏸️ Monitoring disabled")
        print("⏸️  Monitoring disabled")

    def run_scraper_job(self):
        """Execute the scraper (scheduled job)"""
        config = self.load_config()

        # Check if monitoring is still active
        if not config.get("monitoring", {}).get("active", False):
            logger.info("⏸️ Monitoring is paused, skipping scrape")
            print("⏸️  Monitoring is paused, skipping scrape")
            return

        self.is_running = True
        self.last_run_time = time.time()

        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"🔄 Running scheduled scrape at {timestamp}")
            print(f"\n🔄 Running scheduled scrape at {timestamp}")
            run_twitter_scraper()
            self.last_run_status = "success"
            logger.info("✅ Scheduled scrape completed successfully")
            print("✅ Scheduled scrape completed successfully")

            # Calculate next run time
            interval_hours = config.get("monitoring", {}).get("interval_hours", 1)
            self.next_run_time = time.time() + (interval_hours * 3600)

        except Exception as e:
            self.last_run_status = f"error: {str(e)}"
            logger.error(f"❌ Scrape failed: {e}", exc_info=True)
            print(f"❌ Scrape failed: {e}")
        finally:
            self.is_running = False

    def run_once(self):
        """Manually trigger scraper (one-time run)"""
        logger.info("🚀 Manual scrape triggered")
        print("\n🚀 Manual scrape triggered")
        self.run_scraper_job()


def start_scheduler(monitor: ScraperMonitor, interval_hours: int = 1):
    """Start the background scheduler"""
    print(f"⏰ Scheduler started: running every {interval_hours} hour(s)")

    # Schedule the job
    schedule.every(interval_hours).hours.do(monitor.run_scraper_job)

    # Run scheduler loop
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    # Initialize database
    init_database()

    # Create monitor instance
    monitor = ScraperMonitor()

    # Load config
    config = monitor.load_config()
    interval = config.get("monitoring", {}).get("interval_hours", 1)

    print("🤖 PR Lead Scraper Monitor")
    print("=" * 60)
    print(f"Interval: Every {interval} hour(s)")
    print(
        f"Monitoring: {'Active' if config.get('monitoring', {}).get('active') else 'Paused'}"
    )
    print("\nScheduler will run in background...")
    print("Press Ctrl+C to stop\n")

    # Start scheduler (blocks)
    try:
        start_scheduler(monitor, interval_hours=interval)
    except KeyboardInterrupt:
        print("\n\n⏹️  Scheduler stopped")
