import scrapy
import json
from datetime import datetime, timezone

SUBREDDIT = "dataengineering"
MAX_POSTS = 500
SORT = "hot"


class RedditPostItem(scrapy.Item):
    post_id = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    flair = scrapy.Field()
    score = scrapy.Field()
    upvote_ratio = scrapy.Field()
    num_comments = scrapy.Field()
    url = scrapy.Field()
    permalink = scrapy.Field()
    created_utc = scrapy.Field()
    scraped_at = scrapy.Field()


class DataEngineeringSpider(scrapy.Spider):
    name = "dataengineering"

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "ROBOTSTXT_OBEY": False,
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (compatible; DataEngineeringResearch/1.0)",
            "Accept": "application/json",
        },
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scraped_count = 0
        self.after_token = None

    def start_requests(self):
        yield scrapy.Request(self._build_url(), callback=self.parse)

    def _build_url(self):
        base = f"https://www.reddit.com/r/{SUBREDDIT}/{SORT}.json?limit=25&raw_json=1"
        if self.after_token:
            base += f"&after={self.after_token}"
        return base

    def parse(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        posts = data.get("data", {}).get("children", [])
        after = data.get("data", {}).get("after")

        if not posts:
            self.logger.info("No more posts found. Done.")
            return

        for post in posts:
            d = post.get("data", {})
            yield RedditPostItem(
                post_id=d.get("id"),
                title=d.get("title"),
                author=d.get("author"),
                flair=d.get("link_flair_text"),
                score=d.get("score"),
                upvote_ratio=d.get("upvote_ratio"),
                num_comments=d.get("num_comments"),
                url=d.get("url"),
                permalink="https://www.reddit.com" + d.get("permalink", ""),
                created_utc=datetime.fromtimestamp(
                    d.get("created_utc", 0), tz=timezone.utc
                ).isoformat(),
                scraped_at=datetime.now(tz=timezone.utc).isoformat(),
            )
            self.scraped_count += 1

        self.logger.info(f"Scraped {self.scraped_count} posts so far...")

        if after and self.scraped_count < MAX_POSTS:
            self.after_token = after
            yield scrapy.Request(self._build_url(), callback=self.parse)
        else:
            self.logger.info(f"Finished. Total posts scraped: {self.scraped_count}")
