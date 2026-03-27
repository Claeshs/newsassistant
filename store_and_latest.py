from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS articles (
  id                   INTEGER PRIMARY KEY,
  url                  TEXT NOT NULL,         -- original URL (som modtaget)
  url_norm             TEXT NOT NULL,         -- normaliseret URL
  guid                 TEXT,
  url_hash             TEXT NOT NULL,         -- sha256(url_norm)
  title                TEXT,
  source_feed_url      TEXT,
  published_utc        TEXT,                  -- ISO8601 (UTC)
  published_local      TEXT,                  -- ISO8601 (Europe/Copenhagen), valgfrit

  feed_excerpt         TEXT,                  -- uddrag fra feedet (valgfrit)
  feed_excerpt_len     INTEGER,               -- antal tegn i feed-uddrag
  content_md           TEXT,                  -- fuld markdowntekst (valgfrit)
  content_path         TEXT,                  -- sti til md-fil (valgfrit)
  content_len          INTEGER,               -- antal tegn i fuldtekst (fra content_md eller fil)

  checksum             TEXT,                  -- hash af content_md eller filindhold
  first_seen           TEXT NOT NULL DEFAULT (datetime('now')),
  last_seen            TEXT NOT NULL DEFAULT (datetime('now')),
  deleted              INTEGER NOT NULL DEFAULT 0,

  UNIQUE (url_hash) ON CONFLICT IGNORE,
  UNIQUE (guid)     ON CONFLICT IGNORE
);

CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_utc);
CREATE INDEX IF NOT EXISTS idx_articles_last_seen ON articles(last_seen);
CREATE INDEX IF NOT EXISTS idx_articles_deleted ON articles(deleted);
CREATE INDEX IF NOT EXISTS idx_articles_url_norm ON articles(url_norm);
"""

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "mibextid",
    "vero_id",
    "spm",
    "cmpid",
    "ocid",
    "yclid",
    "msclkid",
    "pk_campaign",
    "pk_kwd",
}


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    for stmt in filter(None, SCHEMA.split(";")):
        conn.execute(stmt)
    return conn


def normalize_url(url: str) -> str:
    """
    Normaliserer URL'er for bedre duplikat-detektion:
    - lowercaser scheme/host
    - fjerner standardporte (80 for http, 443 for https)
    - fjerner fragment (#...)
    - sorterer query-parametre og fjerner kendte tracking-parametre
    - trimmer trailing "/"
    """
    if not url:
        return url

    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "http").lower()
    netloc = (parsed.netloc or "").lower()

    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key in TRACKING_PARAMS:
            continue
        query_items.append((key, value))
    query_items.sort(key=lambda item: item[0])
    query = urlencode(query_items, doseq=True)

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", query, ""))


def _calc_checksum_and_length(
    content_md: Optional[str], content_path: Optional[str]
) -> Tuple[Optional[str], Optional[int]]:
    if content_md is not None:
        checksum = hashlib.sha256(content_md.encode("utf-8")).hexdigest() if content_md else None
        return checksum, len(content_md) if content_md else 0

    if content_path and Path(content_path).exists():
        data = Path(content_path).read_bytes()
        checksum = hashlib.sha256(data).hexdigest()
        try:
            text = data.decode("utf-8", errors="replace")
            return checksum, len(text)
        except Exception:
            return checksum, None

    return None, None


def _len_or_none(value: Optional[str]) -> Optional[int]:
    return len(value) if value else None


def upsert_article(conn: sqlite3.Connection, item: Dict[str, Any]) -> int:
    """
    item = {
      "url": str,                        # påkrævet (original)
      "guid": Optional[str],
      "title": Optional[str],
      "source_feed_url": Optional[str],
      "published_utc": Optional[str],    # 'YYYY-MM-DDTHH:MM:SSZ' eller '...+00:00'
      "published_local": Optional[str],  # 'YYYY-MM-DDTHH:MM:SS+01:00'
      "feed_excerpt": Optional[str],     # uddrag fra feed
      "content_md": Optional[str],       # fuldtekst i markdown
      "content_path": Optional[str],     # sti til md-fil
    }
    """
    url = item["url"]
    url_norm = normalize_url(url)
    url_hash = _hash(url_norm)
    guid = item.get("guid") or None

    checksum, content_len = _calc_checksum_and_length(
        item.get("content_md"), item.get("content_path")
    )
    feed_excerpt = item.get("feed_excerpt")
    feed_excerpt_len = _len_or_none(feed_excerpt)

    conn.execute(
        """
        INSERT INTO articles
        (url, url_norm, guid, url_hash, title, source_feed_url, published_utc, published_local,
         feed_excerpt, feed_excerpt_len, content_md, content_path, content_len, checksum,
         first_seen, last_seen, deleted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'), datetime('now'), 0)
        ON CONFLICT(guid) DO UPDATE SET
            title = COALESCE(excluded.title, articles.title),
            source_feed_url = COALESCE(excluded.source_feed_url, articles.source_feed_url),
            published_utc = COALESCE(excluded.published_utc, articles.published_utc),
            published_local = COALESCE(excluded.published_local, articles.published_local),
            feed_excerpt = COALESCE(excluded.feed_excerpt, articles.feed_excerpt),
            feed_excerpt_len = COALESCE(excluded.feed_excerpt_len, articles.feed_excerpt_len),
            content_md = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_md ELSE articles.content_md END,
            content_path = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_path ELSE articles.content_path END,
            content_len = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_len ELSE articles.content_len END,
            checksum = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.checksum ELSE articles.checksum END,
            last_seen = datetime('now'),
            deleted = 0
        ;
        """,
        (
            url,
            url_norm,
            guid,
            url_hash,
            item.get("title"),
            item.get("source_feed_url"),
            item.get("published_utc"),
            item.get("published_local"),
            feed_excerpt,
            feed_excerpt_len,
            item.get("content_md"),
            item.get("content_path"),
            content_len,
            checksum,
        ),
    )

    conn.execute(
        """
        INSERT INTO articles
        (url, url_norm, guid, url_hash, title, source_feed_url, published_utc, published_local,
         feed_excerpt, feed_excerpt_len, content_md, content_path, content_len, checksum,
         first_seen, last_seen, deleted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'), datetime('now'), 0)
        ON CONFLICT(url_hash) DO UPDATE SET
            title = COALESCE(excluded.title, articles.title),
            source_feed_url = COALESCE(excluded.source_feed_url, articles.source_feed_url),
            published_utc = COALESCE(excluded.published_utc, articles.published_utc),
            published_local = COALESCE(excluded.published_local, articles.published_local),
            feed_excerpt = COALESCE(excluded.feed_excerpt, articles.feed_excerpt),
            feed_excerpt_len = COALESCE(excluded.feed_excerpt_len, articles.feed_excerpt_len),
            content_md = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_md ELSE articles.content_md END,
            content_path = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_path ELSE articles.content_path END,
            content_len = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.content_len ELSE articles.content_len END,
            checksum = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum, '')
                    THEN excluded.checksum ELSE articles.checksum END,
            last_seen = datetime('now'),
            deleted = 0
        ;
        """,
        (
            url,
            url_norm,
            guid,
            url_hash,
            item.get("title"),
            item.get("source_feed_url"),
            item.get("published_utc"),
            item.get("published_local"),
            feed_excerpt,
            feed_excerpt_len,
            item.get("content_md"),
            item.get("content_path"),
            content_len,
            checksum,
        ),
    )

    conn.commit()
    cur = conn.execute(
        "SELECT id FROM articles WHERE url_hash=? OR (guid IS NOT NULL AND guid=?) LIMIT 1",
        (url_hash, guid),
    )
    row = cur.fetchone()
    return int(row[0]) if row else -1


def should_skip_scrape(conn: sqlite3.Connection, url: str, guid: Optional[str] = None) -> bool:
    url_norm = normalize_url(url)
    url_hash = _hash(url_norm)
    if guid:
        cur = conn.execute(
            "SELECT 1 FROM articles WHERE guid=? OR url_hash=? LIMIT 1", (guid, url_hash)
        )
    else:
        cur = conn.execute("SELECT 1 FROM articles WHERE url_hash=? LIMIT 1", (url_hash,))
    return cur.fetchone() is not None


def get_articles_missing_content(
    conn: sqlite3.Connection,
    limit: int = 25,
    max_age_hours: Optional[float] = None,
    include_deleted: bool = False,
) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    deleted_clause = "" if include_deleted else "AND deleted=0"
    age_clause = ""
    params: List[Any] = []

    if max_age_hours is not None:
        age_clause = "AND COALESCE(published_utc, last_seen) >= datetime('now', ?)"
        params.append(f"-{float(max_age_hours)} hours")

    params.append(int(limit))

    cur = conn.execute(
        f"""
        SELECT id, url, guid, title, source_feed_url, published_utc, published_local, feed_excerpt
        FROM articles
        WHERE (content_md IS NULL OR TRIM(content_md) = '')
          AND (content_path IS NULL OR TRIM(content_path) = '')
          {deleted_clause}
          {age_clause}
        ORDER BY COALESCE(published_utc, last_seen) DESC
        LIMIT ?
        """,
        params,
    )
    return list(cur.fetchall())


def mark_deleted_by_url(conn: sqlite3.Connection, url: str) -> None:
    url_norm = normalize_url(url)
    conn.execute("UPDATE articles SET deleted=1 WHERE url_hash=?", (_hash(url_norm),))
    conn.commit()


def purge_retention(
    conn: sqlite3.Connection,
    max_age_days: Optional[int] = None,
    keep_latest: Optional[int] = None,
) -> None:
    if max_age_days is not None:
        conn.execute(
            """
            DELETE FROM articles
            WHERE COALESCE(published_utc, last_seen) < datetime('now', ?)
            """,
            (f"-{int(max_age_days)} days",),
        )
    if keep_latest is not None:
        conn.execute(
            f"""
            DELETE FROM articles
            WHERE id NOT IN (
              SELECT id FROM articles
              ORDER BY COALESCE(published_utc, last_seen) DESC
              LIMIT {int(keep_latest)}
            )
            """
        )
    conn.commit()
    conn.execute("VACUUM")


def _select_recent(
    conn: sqlite3.Connection, max_age_hours: float = 48.0, include_deleted: bool = False
) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    deleted_clause = "" if include_deleted else "AND deleted=0"
    cur = conn.execute(
        f"""
        SELECT
            title, url, url_norm, source_feed_url, published_local, published_utc,
            feed_excerpt, feed_excerpt_len, content_md, content_path, content_len
        FROM articles
        WHERE COALESCE(published_utc, last_seen) >= datetime('now', ?)
          {deleted_clause}
        ORDER BY COALESCE(published_utc, last_seen) DESC
        """,
        (f"-{float(max_age_hours)} hours",),
    )
    return list(cur.fetchall())


def generate_latest_md(
    conn: sqlite3.Connection,
    out_path: str,
    max_age_hours: float = 48.0,
    include_deleted: bool = False,
) -> int:
    rows = _select_recent(conn, max_age_hours, include_deleted)
    blocks = []

    for row in rows:
        title = row["title"] or "(uden titel)"
        url = row["url"] or row["url_norm"]
        feed_url = row["source_feed_url"]
        dt_local = row["published_local"]
        dt_utc = row["published_utc"]

        body = None
        if row["content_md"]:
            body = row["content_md"]
        elif row["content_path"] and Path(row["content_path"]).exists():
            try:
                body = Path(row["content_path"]).read_text(encoding="utf-8")
            except Exception:
                body = None
        if not body:
            body = ""

        meta = []
        if url:
            meta.append(f"_Kilde_: {url}")
        if feed_url:
            meta.append(f"_Feed_: {feed_url}")
        if dt_local:
            meta.append(f"_Publiceret (Europe/Copenhagen)_: {dt_local}")
        elif dt_utc:
            meta.append(f"_Publiceret (UTC)_: {dt_utc}")

        lengths = []
        if row["feed_excerpt_len"] is not None:
            lengths.append(f"feed={row['feed_excerpt_len']}")
        if row["content_len"] is not None:
            lengths.append(f"artikel={row['content_len']}")
        if lengths:
            meta.append(f"_Længder_: {', '.join(lengths)}")

        header = f"# {title}\n" + ("\n".join(meta) + "\n\n" if meta else "\n")
        excerpt_block = ""
        if row["feed_excerpt"]:
            excerpt_block = f"> {row['feed_excerpt'].strip()}\n\n"

        blocks.append(header + excerpt_block + body.rstrip() + "\n\n---\n")

    Path(out_path).write_text("".join(blocks), encoding="utf-8")
    return len(rows)


def generate_latest_json(
    conn: sqlite3.Connection,
    out_path: str,
    max_age_hours: float = 48.0,
    include_deleted: bool = False,
) -> int:
    rows = _select_recent(conn, max_age_hours, include_deleted)
    payload = []

    for row in rows:
        content_text = row["content_md"]
        if (not content_text) and row["content_path"] and Path(row["content_path"]).exists():
            try:
                content_text = Path(row["content_path"]).read_text(encoding="utf-8")
            except Exception:
                content_text = None

        payload.append(
            {
                "title": row["title"] or "(uden titel)",
                "url": row["url"] or row["url_norm"],
                "url_norm": row["url_norm"],
                "source_feed_url": row["source_feed_url"],
                "published_local": row["published_local"],
                "published_utc": row["published_utc"],
                "feed_excerpt": row["feed_excerpt"],
                "feed_excerpt_len": row["feed_excerpt_len"],
                "content": content_text,
                "content_len": row["content_len"],
            }
        )

    Path(out_path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return len(payload)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Artikeldatabase: upsert og generér latest.*")
    parser.add_argument("--db", default="articles.db")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    subparsers.add_parser("upsert", help="Upsert artikel fra JSON via stdin")

    md = subparsers.add_parser("md", help="Generér latest.md")
    md.add_argument("--out", default="latest.md")
    md.add_argument("--hours", type=float, default=48.0)
    md.add_argument("--include-deleted", action="store_true")

    js = subparsers.add_parser("json", help="Generér latest.json")
    js.add_argument("--out", default="latest.json")
    js.add_argument("--hours", type=float, default=48.0)
    js.add_argument("--include-deleted", action="store_true")

    sk = subparsers.add_parser("skip", help="Check om scraping kan springes over")
    sk.add_argument("--url", required=True)
    sk.add_argument("--guid")

    dl = subparsers.add_parser("delete", help="Markér som slettet via URL")
    dl.add_argument("--url", required=True)

    pr = subparsers.add_parser("purge", help="Oprydning/retention")
    pr.add_argument("--max-age-days", type=int)
    pr.add_argument("--keep-latest", type=int)

    args = parser.parse_args()
    conn = connect(args.db)

    if args.cmd == "upsert":
        data = json.load(sys.stdin)
        if isinstance(data, dict):
            upsert_article(conn, data)
        elif isinstance(data, list):
            for item in data:
                upsert_article(conn, item)
        else:
            raise SystemExit("JSON skal være et objekt eller en liste.")
        print("OK")
    elif args.cmd == "md":
        count = generate_latest_md(
            conn,
            args.out,
            max_age_hours=args.hours,
            include_deleted=args.include_deleted,
        )
        print(f"Skrev {count} artikler til {args.out}")
    elif args.cmd == "json":
        count = generate_latest_json(
            conn,
            args.out,
            max_age_hours=args.hours,
            include_deleted=args.include_deleted,
        )
        print(f"Skrev {count} artikler til {args.out}")
    elif args.cmd == "skip":
        print("SKIP" if should_skip_scrape(conn, args.url, args.guid) else "FETCH")
    elif args.cmd == "delete":
        mark_deleted_by_url(conn, args.url)
        print("Markeret som slettet")
    elif args.cmd == "purge":
        purge_retention(conn, max_age_days=args.max_age_days, keep_latest=args.keep_latest)
        print("Oprydning udført")


if __name__ == "__main__":
    _cli()
