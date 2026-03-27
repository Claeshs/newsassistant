# store_and_latest.py
from __future__ import annotations
import sqlite3, hashlib, os, time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# =========================
# Skema og DB-initialisering
# =========================

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
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","fbclid","mc_cid","mc_eid","igshid","mibextid","vero_id",
    "spm","cmpid","ocid","yclid","msclkid","pk_campaign","pk_kwd",
}

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    # sikr skema
    for stmt in filter(None, SCHEMA.split(";")):
        conn.execute(stmt)
    return conn

# =========================
# URL-normalisering
# =========================

def normalize_url(url: str) -> str:
    """
    Normaliserer URL'er for bedre duplikat-detektion:
    - lower-caser scheme/host
    - fjerner standardporte (80 for http, 443 for https)
    - fjerner fragment (#...)
    - sorterer query-parametre og fjerner kendte tracking-parametre
    - trimmer trailing "/"
    - konverterer %XX til normaliseret form implicit via parse/unparse
    """
    if not url:
        return url
    u = urlparse(url.strip())
    scheme = (u.scheme or "http").lower()
    netloc = (u.netloc or "").lower()

    # fjern standardporte
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    # rens & sorter query
    q = []
    for k, v in parse_qsl(u.query, keep_blank_values=True):
        if k in TRACKING_PARAMS:
            continue
        q.append((k, v))
    q.sort(key=lambda kv: kv[0])
    query = urlencode(q, doseq=True)

    path = u.path or "/"
    # fjern trailing slash hvis ikke root
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    # byg uden fragment
    new = (scheme, netloc, path, "", query, "")
    return urlunparse(new)

# =========================
# Checksums og længder
# =========================

def _calc_checksum_and_length(content_md: Optional[str],
                              content_path: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Returnerer (checksum, content_len) fra content_md eller content_path.
    """
    if content_md is not None:
        text = content_md
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest() if text else None
        return checksum, (len(text) if text else 0)
    if content_path and Path(content_path).exists():
        data = Path(content_path).read_bytes()
        checksum = hashlib.sha256(data).hexdigest()
        try:
            text = data.decode("utf-8", errors="replace")
            return checksum, len(text)
        except Exception:
            return checksum, None
    return None, None

def _len_or_none(s: Optional[str]) -> Optional[int]:
    return len(s) if s else None

# =========================
# Upsert med betinget indholdsopdatering
# =========================

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
      "content_md": Optional[str],       # fuldtekst MD
      "content_path": Optional[str],     # sti til MD-fil
    }
    """
    url = item["url"]
    url_norm = normalize_url(url)
    url_hash = _hash(url_norm)
    guid = item.get("guid") or None

    checksum, content_len = _calc_checksum_and_length(item.get("content_md"), item.get("content_path"))
    feed_excerpt = item.get("feed_excerpt")
    feed_excerpt_len = _len_or_none(feed_excerpt)

    # Vi bruger INSERT ... ON CONFLICT mod både guid og url_hash
    # Indhold (content_md/content_path/checksum/content_len) opdateres kun,
    # hvis EXCLUDED.checksum er sat og forskellig fra eksisterende.
    conn.execute("""
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
            -- Indhold kun hvis checksum ændret og ikke NULL
            content_md = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_md ELSE articles.content_md END,
            content_path = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_path ELSE articles.content_path END,
            content_len = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_len ELSE articles.content_len END,
            checksum = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.checksum ELSE articles.checksum END,
            last_seen = datetime('now'),
            deleted = 0
        ;
    """, (
        url, url_norm, guid, url_hash, item.get("title"), item.get("source_feed_url"),
        item.get("published_utc"), item.get("published_local"),
        feed_excerpt, feed_excerpt_len, item.get("content_md"), item.get("content_path"),
        content_len, checksum
    ))

    # Hvis guid var NULL eller ikke unik, sikrer vi også konflikt på url_hash:
    conn.execute("""
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
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_md ELSE articles.content_md END,
            content_path = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_path ELSE articles.content_path END,
            content_len = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.content_len ELSE articles.content_len END,
            checksum = CASE
                WHEN excluded.checksum IS NOT NULL AND excluded.checksum != IFNULL(articles.checksum,'')
                    THEN excluded.checksum ELSE articles.checksum END,
            last_seen = datetime('now'),
            deleted = 0
        ;
    """, (
        url, url_norm, guid, url_hash, item.get("title"), item.get("source_feed_url"),
        item.get("published_utc"), item.get("published_local"),
        feed_excerpt, feed_excerpt_len, item.get("content_md"), item.get("content_path"),
        content_len, checksum
    ))

    conn.commit()
    # Hent id for den endelige række:
    cur = conn.execute("SELECT id FROM articles WHERE url_hash=? OR (guid IS NOT NULL AND guid=?) LIMIT 1", (url_hash, guid))
    row = cur.fetchone()
    return int(row[0]) if row else -1

# =========================
# Hjælpefunktioner
# =========================

def should_skip_scrape(conn: sqlite3.Connection, url: str, guid: Optional[str] = None) -> bool:
    """
    Returnerer True, hvis artiklen allerede findes (via normaliseret url eller guid).
    """
    url_norm = normalize_url(url)
    url_hash = _hash(url_norm)
    if guid:
        cur = conn.execute("SELECT 1 FROM articles WHERE guid=? OR url_hash=? LIMIT 1", (guid, url_hash))
    else:
        cur = conn.execute("SELECT 1 FROM articles WHERE url_hash=? LIMIT 1", (url_hash,))
    return cur.fetchone() is not None

def mark_deleted_by_url(conn: sqlite3.Connection, url: str) -> None:
    url_norm = normalize_url(url)
    conn.execute("UPDATE articles SET deleted=1 WHERE url_hash=?", (_hash(url_norm),))
    conn.commit()

def purge_retention(conn: sqlite3.Connection,
                    max_age_days: Optional[int] = None,
                    keep_latest: Optional[int] = None) -> None:
    """
    Slet forældede poster så DB ikke vokser uendeligt.
    - max_age_days: slet alt ældre end X dage (målt på published_utc, fallback last_seen)
    - keep_latest: bevar kun N nyeste (målt på published_utc DESC, fallback last_seen)
    """
    if max_age_days is not None:
        conn.execute("""
            DELETE FROM articles
            WHERE COALESCE(published_utc, last_seen) < datetime('now', ?)
        """, (f'-{int(max_age_days)} days',))
    if keep_latest is not None:
        conn.execute(f"""
            DELETE FROM articles
            WHERE id NOT IN (
              SELECT id FROM articles
              ORDER BY COALESCE(published_utc, last_seen) DESC
              LIMIT {int(keep_latest)}
            )
        """)
    conn.commit()
    conn.execute("VACUUM")

# =========================
# Eksport: latest.md / latest.json
# =========================

def _select_recent(conn: sqlite3.Connection,
                   max_age_hours: float = 48.0,
                   include_deleted: bool = False) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    deleted_clause = "" if include_deleted else "AND deleted=0"
    cur = conn.execute(f"""
        SELECT
            title, url, url_norm, source_feed_url, published_local, published_utc,
            feed_excerpt, feed_excerpt_len, content_md, content_path, content_len
        FROM articles
        WHERE COALESCE(published_utc, last_seen) >= datetime('now', ?)
          {deleted_clause}
        ORDER BY COALESCE(published_utc, last_seen) DESC
    """, (f'-{float(max_age_hours)} hours',))
    return list(cur.fetchall())

def generate_latest_md(conn: sqlite3.Connection,
                       out_path: str,
                       max_age_hours: float = 48.0,
                       include_deleted: bool = False) -> int:
    rows = _select_recent(conn, max_age_hours, include_deleted)

    blocks = []
    for r in rows:
        title = r["title"] or "(uden titel)"
        url = r["url"] or r["url_norm"]
        feed_url = r["source_feed_url"]
        dt_local = r["published_local"]
        dt_utc = r["published_utc"]

        # vælg brødtekst
        body = None
        if r["content_md"]:
            body = r["content_md"]
        elif r["content_path"] and Path(r["content_path"]).exists():
            try:
                body = Path(r["content_path"]).read_text(encoding="utf-8")
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

        lens = []
        if r["feed_excerpt_len"] is not None:
            lens.append(f"feed={r['feed_excerpt_len']}")
        if r["content_len"] is not None:
            lens.append(f"artikel={r['content_len']}")
        if lens:
            meta.append(f"_Længder_: {', '.join(lens)}")

        header = f"# {title}\n" + ("\n".join(meta) + "\n\n" if meta else "\n")
        excerpt_block = ""
        if r["feed_excerpt"]:
            excerpt_block = f"> {r['feed_excerpt'].strip()}\n\n"

        blocks.append(header + excerpt_block + body.rstrip() + "\n\n---\n")

    Path(out_path).write_text("".join(blocks), encoding="utf-8")
    return len(rows)

def generate_latest_json(conn: sqlite3.Connection,
                         out_path: str,
                         max_age_hours: float = 48.0,
                         include_deleted: bool = False) -> int:
    """
    Eksporterer en maskinlæsbar JSON-liste, velegnet til en Custom GPT,
    hvor hver post har titel, links, tider, længder og tekst (hvis til stede).
    """
    import json
    rows = _select_recent(conn, max_age_hours, include_deleted)
    payload = []
    for r in rows:
        # resolv brødtekst lazy
        content_text = r["content_md"]
        if (not content_text) and r["content_path"] and Path(r["content_path"]).exists():
            try:
                content_text = Path(r["content_path"]).read_text(encoding="utf-8")
            except Exception:
                content_text = None

        payload.append({
            "title": r["title"] or "(uden titel)",
            "url": r["url"] or r["url_norm"],
            "url_norm": r["url_norm"],
            "source_feed_url": r["source_feed_url"],
            "published_local": r["published_local"],
            "published_utc": r["published_utc"],
            "feed_excerpt": r["feed_excerpt"],
            "feed_excerpt_len": r["feed_excerpt_len"],
            "content": content_text,
            "content_len": r["content_len"],
        })

    Path(out_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(payload)

# =========================
# Simpel CLI
# =========================

def _cli():
    import argparse
    p = argparse.ArgumentParser(description="Artikeldatabase: upsert og generér latest.*")
    p.add_argument("--db", default="articles.db")
    sub = p.add_subparsers(dest="cmd", required=True)

    # upsert fra stdin (json) for nem integration
    up = sub.add_parser("upsert", help="Upsert artikel fra JSON via stdin")
    # ingen args -> læser stdin

    md = sub.add_parser("md", help="Generér latest.md")
    md.add_argument("--out", default="latest.md")
    md.add_argument("--hours", type=float, default=48.0)
    md.add_argument("--include-deleted", action="store_true")

    js = sub.add_parser("json", help="Generér latest.json")
    js.add_argument("--out", default="latest.json")
    js.add_argument("--hours", type=float, default=48.0)
    js.add_argument("--include-deleted", action="store_true")

    sk = sub.add_parser("skip", help="Check om scraping kan springes over")
    sk.add_argument("--url", required=True)
    sk.add_argument("--guid")

    dl = sub.add_parser("delete", help="Markér som slettet via URL")
    dl.add_argument("--url", required=True)

    pr = sub.add_parser("purge", help="Oprydning/retention")
    pr.add_argument("--max-age-days", type=int)
    pr.add_argument("--keep-latest", type=int)

    args = p.parse_args()
    conn = connect(args.db)

    if args.cmd == "upsert":
        import sys, json
        data = json.load(sys.stdin)
        # tillad enkeltobjekt eller liste
        if isinstance(data, dict):
            upsert_article(conn, data)
        elif isinstance(data, list):
            for item in data:
                upsert_article(conn, item)
        else:
            raise SystemExit("JSON skal være et objekt eller en liste.")
        print("OK")
    elif args.cmd == "md":
        n = generate_latest_md(conn, args.out, max_age_hours=args.hours, include_deleted=args.include_deleted)
        print(f"Skrev {n} artikler til {args.out}")
    elif args.cmd == "json":
        n = generate_latest_json(conn, args.out, max_age_hours=args.hours, include_deleted=args.include_deleted)
        print(f"Skrev {n} artikler til {args.out}")
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