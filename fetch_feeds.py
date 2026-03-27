import json
import os
import time

import feedparser

import store_and_latest

DEFAULT_COUNT = 5
DB_PATH = "articles.db"
FEEDS_FILE = "feeds.json"


def fetch_all_feeds() -> None:
    conn = store_and_latest.connect(DB_PATH)
    total_new_articles = 0

    if not os.path.exists(FEEDS_FILE):
        print(f"Fejl: Filen '{FEEDS_FILE}' blev ikke fundet.")
        return

    with open(FEEDS_FILE, "r", encoding="utf-8") as file_handle:
        feeds = json.load(file_handle)

    for feed_info in feeds:
        name = feed_info["name"]
        url = feed_info["url"]
        count = feed_info.get("count", DEFAULT_COUNT)

        print(f"Henter feed: {name} ... ", end="", flush=True)

        try:
            parsed = feedparser.parse(url)
            entries = parsed.entries[:count]
            new_in_feed = 0

            for entry in entries:
                link = entry.get("link")
                if not link:
                    continue

                guid = entry.get("id", link)

                if store_and_latest.should_skip_scrape(conn, link, guid):
                    continue

                published_utc = None
                if "published_parsed" in entry and entry.published_parsed:
                    published_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", entry.published_parsed)
                elif "updated_parsed" in entry and entry.updated_parsed:
                    published_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", entry.updated_parsed)

                item = {
                    "url": link,
                    "guid": guid,
                    "title": entry.get("title", ""),
                    "source_feed_url": url,
                    "published_utc": published_utc,
                    "feed_excerpt": entry.get("summary", ""),
                }

                store_and_latest.upsert_article(conn, item)
                new_in_feed += 1
                total_new_articles += 1

            print(f"Fandt {new_in_feed} nye artikler ud af {len(entries)} tjekkede.")

        except Exception as exc:
            print(f"Fejl under hentning: {exc}")

    print(f"\nFærdig! {total_new_articles} nye artikler blev gemt i {DB_PATH}.")


if __name__ == "__main__":
    fetch_all_feeds()
