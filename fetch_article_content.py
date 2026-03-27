import argparse
import time

import requests
import trafilatura

import store_and_latest

DB_PATH = "articles.db"
DEFAULT_LIMIT = 20
DEFAULT_TIMEOUT = 20

USER_AGENT = (
    "NewsAssistant/1.0 "
    "(https://example.invalid; personlig nyhedsassistent til privat kuratering)"
)


def download_article_markdown(url: str, timeout: int = DEFAULT_TIMEOUT) -> str | None:
    response = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    response.raise_for_status()

    html = response.text.strip()
    if not html:
        return None

    extracted = trafilatura.extract(
        html,
        url=response.url,
        output_format="markdown",
        include_links=True,
        include_formatting=True,
        favor_recall=True,
        deduplicate=True,
    )
    return extracted.strip() if extracted else None


def fetch_article_content(
    db_path: str,
    limit: int,
    max_age_hours: float | None,
    timeout: int,
    sleep_seconds: float,
) -> None:
    conn = store_and_latest.connect(db_path)
    rows = store_and_latest.get_articles_missing_content(
        conn,
        limit=limit,
        max_age_hours=max_age_hours,
    )

    if not rows:
        print("Ingen artikler mangler indhold.")
        return

    fetched = 0
    skipped = 0
    failed = 0

    for index, row in enumerate(rows, start=1):
        title = row["title"] or "(uden titel)"
        print(f"[{index}/{len(rows)}] Henter artikeltekst: {title}")

        try:
            content_md = download_article_markdown(row["url"], timeout=timeout)
            if not content_md:
                skipped += 1
                print("  Ingen brugbar artikeltekst fundet.")
                continue

            store_and_latest.upsert_article(
                conn,
                {
                    "url": row["url"],
                    "guid": row["guid"],
                    "title": row["title"],
                    "source_feed_url": row["source_feed_url"],
                    "published_utc": row["published_utc"],
                    "published_local": row["published_local"],
                    "feed_excerpt": row["feed_excerpt"],
                    "content_md": content_md,
                },
            )
            fetched += 1
            print(f"  Gemt {len(content_md)} tegn.")
        except requests.RequestException as exc:
            failed += 1
            print(f"  Netværksfejl: {exc}")
        except Exception as exc:
            failed += 1
            print(f"  Fejl ved udtræk: {exc}")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    print(
        f"\nFærdig! {fetched} artikler gemt, {skipped} uden brugbar tekst, {failed} fejl."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Hent fuld artikeltekst for artikler i databasen, der endnu mangler indhold."
    )
    parser.add_argument("--db", default=DB_PATH, help="Sti til SQLite-databasen")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maksimalt antal artikler at hente pr. kørsel",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=48.0,
        help="Hent kun artikler fra de seneste X timer",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Timeout i sekunder pr. HTTP-kald",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Pause mellem forespørgsler i sekunder",
    )

    args = parser.parse_args()
    fetch_article_content(
        db_path=args.db,
        limit=args.limit,
        max_age_hours=args.hours,
        timeout=args.timeout,
        sleep_seconds=args.sleep,
    )
