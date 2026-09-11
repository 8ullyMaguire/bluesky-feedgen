#!/usr/bin/env python3
"""Leftist list top-posts feed generator (stdlib only).

Serves many feeds from members of the Leftist curate-list. Each feed is
configured by its own time window, engagement thresholds, ranking mode and
optional scoring override (see feedgen.json).

Endpoints on port 8004 behind leftist.polarisocial.xyz:
  /.well-known/did.json
  /xrpc/app.bsky.feed.describeFeedGenerator
  /xrpc/app.bsky.feed.getFeedSkeleton?feed=<feed-uri>&limit=&cursor=
  /  (human status page for all feeds, with per-feed and per-gate counts)

DESIGN NOTES
------------
Dedup is a *bounded suppression window*, not a permanent "seen" set. A post
that appears in a feed is suppressed in that feed for `seen_ttl_hours`
(default 24). After that it can return if it is still inside the feed's age
window. Rationale:

  - The original implementation marked a post seen at pick time, forever.
    Since the candidate pool is finite and posts age out of the windows
    anyway, every feed drained to 0-4 posts and stayed there.
  - Bluesky does not tell a feed generator *who* is asking
    (`getFeedSkeleton` carries no requester identity), so genuine per-user
    "hide after this user liked it" is impossible here. A bounded global
    suppression is the closest honest approximation: it stops a post
    flickering while someone has the feed open, without draining the pool.
  - `seen_ttl_hours: 0` disables dedup entirely (pure stable board).
  - `seen_ttl_hours` >= the feed's window approximates the old once-ever
    behaviour without the unbounded state growth.

The "repost share" rule is approximate: a post qualifies only if its repost
count is at least `min_reposts_share` of its like count (default 0.33).

Save/bookmark counts are structurally unavailable: Bluesky bookmarks are
private per user and no public save count exists in any AppView response.
The `w_saves` weight therefore always multiplies zero. It is kept in the
config so the intent is visible, and so the term starts working if Bluesky
ever exposes the number.

State lives in SQLite (feedgen.sqlite) so the action bot, the list updater
and this service share one store and nothing rewrites a 1.6 MB JSON file per
pick. `feedgen-state.json` is migrated once on first start and kept as a
backup.
"""

import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))


def load_json(path):
    with open(os.path.join(BASE, path)) as f:
        return json.load(f)


def load_dotenv(path):
    vals = {}
    try:
        with open(os.path.join(BASE, path)) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return vals


CFG = load_json("feedgen.json")
ENV = load_dotenv(".env") | dict(os.environ)

HOSTNAME = CFG["hostname"]
SERVICE_DID = f"did:web:{HOSTNAME}"
PUBLISHER_DID = CFG["publisher_did"]
FEEDS = {
    f"at://{PUBLISHER_DID}/app.bsky.feed.generator/{f['rkey']}": f for f in CFG["feeds"]
}

DB_PATH = os.environ.get("FEEDGEN_DB") or os.path.join(BASE, "feedgen.sqlite")
LEGACY_STATE_PATH = os.environ.get("FEEDGEN_STATE_PATH") or os.path.join(
    BASE, "feedgen-state.json"
)

CACHE: dict = {
    # NOTE: there is deliberately no "members" field. The previous version set
    # it to `list_items` (posts scanned), which read as "the list has 10,000
    # members" and sent everyone chasing the wrong cause of the empty feeds.
    # The list's real member count comes from app.bsky.graph.getList (765 as of
    # 2026-09-11) and is not needed by the serving path.
    "feeds": {}, "updated": 0, "scanned": 0, "error": None,
    "gen": 0, "stats": {}, "state_uris": 0, "taste_authors": 0,
}
CACHE_LOCK = threading.Lock()
DB_LOCK = threading.Lock()

_db_con = None


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    uri           TEXT PRIMARY KEY,
    cid           TEXT,
    author_did    TEXT NOT NULL,
    author_handle TEXT,
    indexed_at    TEXT NOT NULL,
    like_count    INTEGER DEFAULT 0,
    repost_count  INTEGER DEFAULT 0,
    quote_count   INTEGER DEFAULT 0,
    reply_count   INTEGER DEFAULT 0,
    text          TEXT DEFAULT '',
    langs         TEXT DEFAULT '',
    record_json   TEXT NOT NULL,
    first_seen    REAL NOT NULL,
    last_seen     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_posts_author  ON posts(author_did);
CREATE INDEX IF NOT EXISTS idx_posts_indexed ON posts(indexed_at);

CREATE TABLE IF NOT EXISTS seen (
    rkey       TEXT NOT NULL,
    uri        TEXT NOT NULL,
    shown_at   REAL NOT NULL,
    shown_count INTEGER DEFAULT 1,
    PRIMARY KEY (rkey, uri)
);
CREATE INDEX IF NOT EXISTS idx_seen_shown ON seen(rkey, shown_at);

CREATE TABLE IF NOT EXISTS author_ml (
    author_did  TEXT PRIMARY KEY,
    ml_posts    INTEGER DEFAULT 0,
    total_posts INTEGER DEFAULT 0,
    affinity    REAL DEFAULT 0,
    updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS taste (
    author_did  TEXT PRIMARY KEY,
    likes_count INTEGER DEFAULT 0,
    updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS actions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,
    uri        TEXT NOT NULL,
    cid        TEXT,
    author_did TEXT,
    created_at REAL NOT NULL,
    status     TEXT NOT NULL,
    detail     TEXT,
    UNIQUE(kind, uri)
);
CREATE INDEX IF NOT EXISTS idx_actions_created ON actions(kind, created_at);

CREATE TABLE IF NOT EXISTS promotions (
    did            TEXT PRIMARY KEY,
    handle         TEXT,
    promoter       TEXT,
    promotion_type TEXT,
    evidence_uri   TEXT,
    discovered_at  REAL,
    added_at       REAL,
    listitem_rkey  TEXT,
    status         TEXT DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);

CREATE TABLE IF NOT EXISTS follows (
    actor_did   TEXT NOT NULL,
    subject_did TEXT NOT NULL,
    handle      TEXT,
    direction   TEXT NOT NULL,
    seen_at     REAL NOT NULL,
    PRIMARY KEY (actor_did, subject_did, direction)
);

CREATE TABLE IF NOT EXISTS bot_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mode            TEXT NOT NULL,
    started         REAL NOT NULL,
    finished        REAL,
    candidates      INTEGER DEFAULT 0,
    acted           INTEGER DEFAULT 0,
    dry_run         INTEGER DEFAULT 1,
    note            TEXT
);
"""


def db():
    global _db_con
    if _db_con is None:
        con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=30000")
        con.executescript(SCHEMA)
        _db_con = con
    return _db_con


def meta_get(k, default=None):
    try:
        row = db().execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else default
    except sqlite3.Error:
        return default


def meta_set(k, v):
    db().execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (k, str(v)),
    )


def migrate_legacy_state():
    """Import the old JSON seen-set once: {rkey: [uri, ...]} -> seen table.

    Imported entries get shown_at = now, so they stay suppressed for
    `seen_ttl_hours` and then are free to return (instead of being permanent).
    """
    if meta_get("legacy_state_migrated") == "1":
        return 0
    n = 0
    try:
        with open(LEGACY_STATE_PATH) as fh:
            snap = json.load(fh)
        now = time.time()
        with DB_LOCK:
            for rkey, uris in snap.items():
                if not isinstance(uris, list):
                    continue
                rows = [(rkey, u, now, 1) for u in uris if isinstance(u, str)]
                db().executemany(
                    "INSERT OR IGNORE INTO seen(rkey,uri,shown_at,shown_count) "
                    "VALUES(?,?,?,?)", rows)
                n += len(rows)
        if n and os.path.exists(LEGACY_STATE_PATH):
            backup = LEGACY_STATE_PATH + ".migrated"
            if not os.path.exists(backup):
                os.replace(LEGACY_STATE_PATH, backup)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        print(f"[feedgen] legacy state migration skipped: {e}", flush=True)
    meta_set("legacy_state_migrated", "1")
    print(f"[feedgen] migrated {n} legacy seen entries into sqlite", flush=True)
    return n


def prune_state(now_ts=None):
    """Drop suppression rows that are long past any feed's TTL."""
    now_ts = now_ts or time.time()
    longest = max([f.get("seen_ttl_hours", 24) for f in CFG["feeds"]] + [24])
    cutoff = now_ts - (longest * 3600 + 3600)
    with DB_LOCK:
        cur = db().execute("DELETE FROM seen WHERE shown_at < ?", (cutoff,))
        db().execute("DELETE FROM posts WHERE last_seen < ?", (now_ts - 90 * 86400,))
    return cur.rowcount


# --------------------------------------------------------------------------
# atproto
# --------------------------------------------------------------------------

def rpc(host, method, data=None, token=None, timeout=30):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", "User-Agent": "leftist-feedgen/2.0"}
        | ({"Authorization": f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} HTTP {e.code}: {e.read().decode()[:200]}")


def parse_time(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


# --------------------------------------------------------------------------
# scoring
# --------------------------------------------------------------------------

def weights_for(fcfg):
    """Global scoring block, with an optional per-feed override."""
    w = dict(CFG["scoring"])
    w.update(fcfg.get("scoring") or {})
    return w


def base_score(p, w):
    """Engagement score. `saves` is structurally 0 — see module docstring."""
    return (
        p.get("likeCount", 0) * w.get("w_likes", 1.0)
        + p.get("repostCount", 0) * w.get("w_reposts", 3.0)
        + p.get("quoteCount", 0) * w.get("w_quotes", 2.0)
        + p.get("replyCount", 0) * w.get("w_replies", 0.5)
        + 0 * w.get("w_saves", 0.0)
    )


def ml_keyword_hits(text, keywords):
    if not text:
        return 0
    low = text.lower()
    return sum(1 for k in keywords if k in low)


def rank_score(p, fcfg, w, age_h, ctx):
    """Apply the feed's ranking mode + affinity modifiers to a base score."""
    s = base_score(p, w)
    did = (p.get("author") or {}).get("did") or "?"
    mode = fcfg.get("ranking", "flat")
    mlw = fcfg.get("ml_weight", 0.0)
    taw = fcfg.get("taste_weight", 0.0)

    if mode in ("foryou", "ml") and (mlw or taw):
        ml = ctx["ml_affinity"].get(did, 0.0)
        ta = ctx["taste"].get(did, 0.0)
        if did in ctx["ml_seeds"]:
            ml = 1.0
        s = s * (1.0 + mlw * ml) * (1.0 + taw * ta)
        if mode == "ml" and did in ctx["ml_seeds"]:
            s += fcfg.get("ml_seed_bonus", 0.0)
        text = (p.get("record") or {}).get("text", "") if isinstance(
            p.get("record"), dict) else ""
        hits = ml_keyword_hits(text, ctx["ml_keywords"])
        if hits:
            s *= 1.0 + min(hits, 3) * fcfg.get("ml_keyword_bonus", 0.0)

    decay = fcfg.get("decay_factor", 0.8)
    if mode == "trending" and age_h > 0:
        s = s / (1 + age_h ** decay)
    elif mode in ("deep", "foryou", "ml"):
        floor = fcfg.get("age_floor_hours", 0.25)
        s = s / (max(age_h, floor) ** decay)
    return s


def select(posts, fcfg, now, owner, ctx, suppressed):
    """Rank the posts for one feed. Returns (uris, stats).

    `suppressed` is the set of URIs currently inside this feed's suppression
    window (written back by the caller when the run finishes).
    """
    w = weights_for(fcfg)
    rkey = fcfg["rkey"]
    ttl = fcfg.get("seen_ttl_hours", 24)
    owner_did = (owner or {}).get("did")
    gate_mode = fcfg.get("gate_mode", "all")
    stats = {"in_window": 0, "pass_share": 0, "pass_gates": 0,
             "suppressed": 0, "final": 0, "owner": 0}
    cands = []

    for p in posts:
        ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
        if not ts:
            continue
        try:
            age_h = (now - parse_time(ts)).total_seconds() / 3600
        except (ValueError, TypeError):
            continue
        if not (fcfg.get("min_age_hours", 0) <= age_h <= fcfg["max_age_hours"]):
            continue
        stats["in_window"] += 1

        did = (p.get("author") or {}).get("did")
        is_owner = bool(owner.get("always_include") and owner_did and did == owner_did)
        likes = p.get("likeCount", 0)
        reposts = p.get("repostCount", 0)

        # --- owner posts bypass every gate, including suppression ---
        if not is_owner:
            share = fcfg.get("min_reposts_share") or 0
            if share and (likes <= 0 or reposts < likes * share):
                continue
            stats["pass_share"] += 1
            s = rank_score(p, fcfg, w, age_h, ctx)
            gates = []
            if (fcfg.get("min_score") or 0) > 0:
                gates.append(s >= fcfg["min_score"])
            if (fcfg.get("min_likes") or 0) > 0:
                gates.append(likes >= fcfg["min_likes"])
            if gates:
                ok = all(gates) if gate_mode == "all" else any(gates)
                if not ok:
                    continue
            stats["pass_gates"] += 1
            uri = p.get("uri")
            if not uri:
                continue
            if ttl and uri in suppressed:
                stats["suppressed"] += 1
                continue
        else:
            s = rank_score(p, fcfg, w, age_h, ctx)
            s = s * owner.get("boost", 1.0) + owner.get("bonus", 0.0)
            stats["owner"] += 1
            stats["pass_share"] += 1
            stats["pass_gates"] += 1
            uri = p.get("uri")
            if not uri:
                continue

        cands.append((s, uri, did))

    # Deterministic order: score desc, then URI. Same inputs -> same board,
    # so a refresh does not reshuffle a subscriber's feed.
    cands.sort(key=lambda t: (-t[0], t[1]))

    cap = fcfg.get("max_per_author", 0) or 0
    out, per_author = [], {}
    for s, uri, did in cands:
        if len(out) >= fcfg.get("max_posts", 100):
            break
        if did != owner_did and cap and per_author.get(did, 0) >= cap:
            continue
        per_author[did] = per_author.get(did, 0) + 1
        out.append(uri)
    stats["final"] = len(out)
    return out, stats


# --------------------------------------------------------------------------
# fetch
# --------------------------------------------------------------------------

OWNER_POST_PAGES = 20


def fetch_all(token):
    """One full read pass: list feed + owner posts. Returns (posts, stats)."""
    src = CFG["source"]
    appview = src["appview_host"]
    owner_did = CFG.get("owner", {}).get("did")
    now = datetime.now(timezone.utc)
    max_window_h = max(f.get("max_age_hours", 48) for f in CFG["feeds"])
    cutoff_ts = (now - timedelta(hours=max_window_h)).isoformat()

    list_items = 0
    pages = 0
    owner_posts = 0
    by_uri = {}

    cursor = None
    for page in range(200):
        q = urllib.parse.urlencode(
            {"list": src["list_uri"], "limit": 100}
            | ({"cursor": cursor} if cursor else {}))
        try:
            d = rpc(appview, f"app.bsky.feed.getListFeed?{q}", token=token, timeout=45)
        except RuntimeError as e:
            print(f"[feedgen] getListFeed page {page} failed: {e}", flush=True)
            break
        items = d.get("feed", [])
        if not items:
            break
        list_items += len(items)
        pages += 1
        for item in items:
            if "reason" in item and item["reason"] != "DIRECT":
                continue
            p = item.get("post")
            if not p:
                continue
            ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
            if ts and ts >= cutoff_ts:
                by_uri[p["uri"]] = p
        cursor = d.get("cursor")
        if not cursor:
            break

    if owner_did:
        cursor = None
        for _ in range(OWNER_POST_PAGES):
            q = urllib.parse.urlencode(
                {"actor": owner_did, "limit": 100}
                | ({"cursor": cursor} if cursor else {}))
            try:
                d = rpc(appview, f"app.bsky.feed.getAuthorFeed?{q}", token=token, timeout=45)
            except RuntimeError:
                break
            items = d.get("feed", [])
            if not items:
                break
            for i in items:
                if src.get("include_reposts") or "reason" not in i:
                    p = i.get("post")
                    if not p:
                        continue
                    ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
                    if ts and ts >= cutoff_ts and p["uri"] not in by_uri:
                        by_uri[p["uri"]] = p
                        owner_posts += 1
            cursor = d.get("cursor")
            if not cursor:
                break

    return list(by_uri.values()), {
        "list_items": list_items, "pages": pages, "owner_posts": owner_posts,
    }


def record_posts(posts):
    now = time.time()
    rows = []
    for p in posts:
        rec = p.get("record") if isinstance(p.get("record"), dict) else {}
        langs = rec.get("langs") or []
        rows.append((
            p.get("uri"), p.get("cid"), (p.get("author") or {}).get("did", "?"),
            (p.get("author") or {}).get("handle", ""),
            p.get("indexedAt") or rec.get("createdAt") or "",
            p.get("likeCount", 0), p.get("repostCount", 0),
            p.get("quoteCount", 0), p.get("replyCount", 0),
            (rec.get("text") or "")[:2000], ",".join(langs) if isinstance(langs, list) else "",
            json.dumps(p), now, now,
        ))
    with DB_LOCK:
        db().executemany(
            """INSERT INTO posts(uri,cid,author_did,author_handle,indexed_at,
                   like_count,repost_count,quote_count,reply_count,text,langs,
                   record_json,first_seen,last_seen)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(uri) DO UPDATE SET
                 cid=excluded.cid, like_count=excluded.like_count,
                 repost_count=excluded.repost_count, quote_count=excluded.quote_count,
                 reply_count=excluded.reply_count, text=excluded.text,
                 langs=excluded.langs, record_json=excluded.record_json,
                 last_seen=excluded.last_seen""", rows)
    return len(rows)


def build_ml_affinity():
    """Per-author ML affinity from the corpus we already stored.

    affinity = share of the author's stored posts that hit an ML keyword.
    Config `ml_seeds` (handles or DIDs) are pinned to 1.0.
    """
    keywords = [k.lower() for k in CFG.get("ml_keywords", [])]
    aff = {}
    if keywords:
        rows = db().execute(
            "SELECT author_did, text FROM posts WHERE last_seen > ?",
            (time.time() - 90 * 86400,)).fetchall()
        tot, hits = {}, {}
        for did, text in rows:
            tot[did] = tot.get(did, 0) + 1
            low = (text or "").lower()
            if any(k in low for k in keywords):
                hits[did] = hits.get(did, 0) + 1
        for did, n in tot.items():
            aff[did] = round(hits.get(did, 0) / n, 4) if n else 0.0
    seeds = set()
    # resolve seed handles -> dids (public, cheap)
    for s in CFG.get("ml_seeds", []) or []:
        if s.startswith("did:"):
            seeds.add(s)
            continue
        try:
            q = urllib.parse.urlencode({"handle": s})
            r = rpc(CFG["source"]["appview_host"],
                    f"com.atproto.identity.resolveHandle?{q}")
            if r.get("did"):
                seeds.add(r["did"])
        except Exception:
            continue
    for did in seeds:
        aff[did] = max(aff.get(did, 0.0), 1.0)
    return aff, seeds


def build_taste(token):
    """Owner's own likes -> affinity toward the authors they actually like.

    This is the "learns from your tastes" half of the ML feeds. It reads only
    the owner's own public likes. NOTE: `app.bsky.feed.getActorLikes` is served
    by the PDS (bsky.social) with auth; public.api.bsky.app answers
    "Profile not found" for it.
    """
    owner_did = CFG.get("owner", {}).get("did")
    if not owner_did:
        return {}
    # Taste changes slowly; rebuilding it every refresh costs ~20 pages of API.
    ttl_h = float(CFG.get("taste_refresh_hours", 6))
    cached_at = meta_get("taste_built_at")
    rows = db().execute("SELECT author_did, likes_count FROM taste").fetchall()
    if rows and cached_at and (time.time() - float(cached_at)) < ttl_h * 3600:
        top = max(r[1] for r in rows) or 1
        return {r[0]: round(r[1] / top, 4) for r in rows}
    counts, cursor, pages = {}, None, 0
    host = CFG["source"]["pds_host"]
    try:
        while pages < 10:
            q = urllib.parse.urlencode(
                {"actor": owner_did, "limit": 100}
                | ({"cursor": cursor} if cursor else {}))
            d = rpc(host, f"app.bsky.feed.getActorLikes?{q}", token=token, timeout=45)
            items = d.get("feed", [])
            if not items:
                break
            for it in items:
                did = ((it.get("post") or {}).get("author") or {}).get("did")
                if did:
                    counts[did] = counts.get(did, 0) + 1
            cursor = d.get("cursor")
            pages += 1
            if not cursor:
                break
    except Exception as e:
        print(f"[feedgen] taste signal unavailable: {e}", flush=True)
        return {}
    if not counts:
        return {}
    top = max(counts.values())
    taste = {did: round(n / top, 4) for did, n in counts.items()}
    now = time.time()
    with DB_LOCK:
        db().execute("DELETE FROM taste")
        db().executemany(
            "INSERT OR REPLACE INTO taste(author_did,likes_count,updated_at) VALUES(?,?,?)",
            [(did, n, now) for did, n in counts.items()])
    meta_set("taste_built_at", now)
    return taste


# --------------------------------------------------------------------------
# refresh
# --------------------------------------------------------------------------

def refresh():
    src = CFG["source"]
    handle = ENV.get("BSKY_HANDLE")
    password = ENV.get("BSKY_APP_PASSWORD")
    if not handle or not password:
        raise RuntimeError("missing BSKY_HANDLE/BSKY_APP_PASSWORD in feedgen .env")
    sess = rpc(src["pds_host"], "com.atproto.server.createSession",
               {"identifier": handle, "password": password})
    token = sess["accessJwt"]

    now = datetime.now(timezone.utc)
    posts, fstats = fetch_all(token)
    record_posts(posts)

    ml_affinity, ml_seeds = build_ml_affinity()
    taste = build_taste(token)
    ctx = {
        "ml_affinity": ml_affinity, "ml_seeds": ml_seeds, "taste": taste,
        "ml_keywords": [k.lower() for k in CFG.get("ml_keywords", [])],
    }

    owner = CFG.get("owner", {})
    picks, stats = {}, {}
    shown = []
    now_ts = time.time()
    for f in CFG["feeds"]:
        ttl = f.get("seen_ttl_hours", 24)
        suppressed = set()
        if ttl:
            rows = db().execute(
                "SELECT uri FROM seen WHERE rkey=? AND shown_at > ?",
                (f["rkey"], now_ts - ttl * 3600)).fetchall()
            suppressed = {r[0] for r in rows}
        uris, st = select(posts, f, now, owner, ctx, suppressed)
        picks[f["rkey"]] = uris
        stats[f["rkey"]] = st | {"suppressed_set": len(suppressed), "ttl": ttl}
        shown.extend((f["rkey"], u) for u in uris)

    if shown:
        with DB_LOCK:
            db().executemany(
                """INSERT INTO seen(rkey,uri,shown_at,shown_count) VALUES(?,?,?,1)
                   ON CONFLICT(rkey,uri) DO UPDATE SET
                     shown_at=excluded.shown_at,
                     shown_count=seen.shown_count+1""",
                [(r, u, now_ts) for r, u in shown])
    prune_state(now_ts)

    meta_set("last_refresh", now_ts)
    with CACHE_LOCK:
        total_seen = db().execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    fstats["state_uris"] = total_seen
    fstats["taste_authors"] = len(taste)
    fstats["ml_authors"] = len(ml_affinity)
    fstats["posts_stored"] = len(posts)
    return picks, stats, fstats


def refresh_loop():
    while True:
        try:
            picks, stats, fstats = refresh()
            with CACHE_LOCK:
                CACHE.update(feeds=picks, stats=stats, updated=time.time(),
                             scanned=fstats["posts_stored"], error=None,
                             gen=CACHE["gen"] + 1, **{
                                 k: fstats[k] for k in
                                 ("state_uris", "taste_authors", "ml_authors")})
                CACHE["fetch"] = fstats
            per_feed = " ".join(f"{k}={len(v)}" for k, v in picks.items())
            print(f"[feedgen] refresh ok: total={sum(len(v) for v in picks.values())} "
                  f"{per_feed} | scanned={fstats['list_items']} "
                  f"pages={fstats['pages']} owner={fstats['owner_posts']} "
                  f"state={fstats['state_uris']}", flush=True)
        except Exception as e:
            with CACHE_LOCK:
                CACHE["error"] = f"{type(e).__name__}: {e}"[:300]
            print(f"[feedgen] refresh failed: {e}", flush=True)
        time.sleep(CFG.get("refresh_secs", 600))


# --------------------------------------------------------------------------
# http
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "leftist-feedgen/3.0"

    def _send(self, code, obj, ctype="application/json", headers=None):
        body = json.dumps(obj).encode() if isinstance(obj, (dict, list)) else obj
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)

        if u.path == "/.well-known/did.json":
            return self._send(200, {
                "@context": ["https://www.w3.org/ns/did/v1"],
                "id": SERVICE_DID,
                "service": [{"id": "#bsky_fg", "type": "BskyFeedGenerator",
                             "serviceEndpoint": f"https://{HOSTNAME}"}],
            })

        if u.path == "/xrpc/app.bsky.feed.describeFeedGenerator":
            return self._send(200, {"did": SERVICE_DID,
                                    "feeds": [{"uri": uri} for uri in FEEDS]})

        if u.path == "/xrpc/app.bsky.feed.getFeedSkeleton":
            uri = q.get("feed", [None])[0]
            if uri not in FEEDS:
                return self._send(400, {"error": "UnknownFeed", "message": "unknown feed"})
            try:
                limit = max(1, min(100, int(q.get("limit", ["50"])[0])))
            except ValueError:
                limit = 50
            raw = q.get("cursor", ["0"])[0]
            gen_of_cursor = None
            if ":" in raw:
                gen_of_cursor, _, raw = raw.partition(":")
            try:
                offset = max(0, int(raw))
            except ValueError:
                offset = 0
            with CACHE_LOCK:
                picks = list(CACHE["feeds"].get(FEEDS[uri]["rkey"], []))
                gen = CACHE["gen"]
            page = picks[offset:offset + limit]
            out = {"feed": [{"post": p} for p in page]}
            # Generational cursor: a client paging across a refresh gets
            # current-generation data rather than a silently shifted window.
            if offset + limit < len(picks):
                out["cursor"] = f"{gen}:{offset + limit}"
            elif gen_of_cursor is not None and gen_of_cursor != str(gen):
                out["cursor"] = f"{gen}:{offset}"
            return self._send(200, out, headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache"})

        if u.path in ("/", "/health"):
            with CACHE_LOCK:
                snap = {k: v for k, v in CACHE.items()}
                snap["feeds"] = {rk: len(v) for rk, v in CACHE["feeds"].items()}
                snap["stats"] = {k: dict(v) for k, v in CACHE["stats"].items()}
            rows = "".join(
                f"<li><code>{f['rkey']}</code> ({f['display_name']}): "
                f"<b>{snap['feeds'].get(f['rkey'], 0)}</b> posts</li>"
                for f in CFG["feeds"])
            stat_rows = "".join(
                "<tr><td>{rk}</td><td>{in_window}</td><td>{pass_share}</td>"
                "<td>{pass_gates}</td><td>{suppressed}</td><td>{owner}</td>"
                "<td>{final}</td><td>{ttl}h</td></tr>".format(rk=k, **v)
                for k, v in snap["stats"].items())
            fetch = snap.get("fetch", {})
            html = (
                f"<h1>Leftist top-posts feeds</h1><ul>{rows}</ul>"
                f"<h2>per-feed pipeline</h2>"
                f"<table border=1 cellpadding=4><tr><th>feed</th><th>in_window</th>"
                f"<th>pass_share</th><th>pass_gates</th><th>suppressed</th>"
                f"<th>owner</th><th>final</th><th>ttl</th></tr>{stat_rows}</table>"
                f"<p>scanned={snap['scanned']} list_items={fetch.get('list_items','?')} "
                f"pages={fetch.get('pages','?')} owner_posts={fetch.get('owner_posts','?')} "
                f"updated={snap['updated']:.0f} gen={snap.get('gen','?')} "
                f"error={snap['error']}</p>"
                f"<p>state_rows={snap.get('state_uris','?')} "
                f"ml_authors={snap.get('ml_authors','?')} "
                f"taste_authors={snap.get('taste_authors','?')} "
                f"db={os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0} bytes</p>"
            ).encode()
            return self._send(200, html, "text/html")

        return self._send(404, {"error": "NotFound"})

    def log_message(self, *a):
        pass


def main():
    migrate_legacy_state()
    print("[feedgen] warming cache (first refresh)...", flush=True)
    try:
        picks, stats, fstats = refresh()
        with CACHE_LOCK:
            CACHE.update(feeds=picks, stats=stats, updated=time.time(),
                         scanned=fstats["posts_stored"], gen=1,
                         **{k: fstats[k] for k in
                            ("state_uris", "taste_authors", "ml_authors")})
            CACHE["fetch"] = fstats
        print(f"[feedgen] warm ok: {sum(len(v) for v in picks.values())} picks "
              f"({', '.join(f'{k}={len(v)}' for k, v in picks.items())})", flush=True)
    except Exception as e:
        CACHE["error"] = f"warm failed: {e}"[:300]
        print(f"[feedgen] warm failed (serving empty until refresh): {e}", flush=True)
    threading.Thread(target=refresh_loop, daemon=True).start()
    HTTPServer(("127.0.0.1", CFG.get("port", 8004)), Handler).serve_forever()


if __name__ == "__main__":
    main()
