#!/usr/bin/env python3
"""Leftist list top-posts feed generator (stdlib only), modeled on 'Popular Hoy'.

Serves multiple feeds from members of the Leftist curate-list. Each feed is
configured by its own time window, engagement thresholds, and ranking mode
(see feedgen.json).

Endpoints on port 8004 behind leftist.polarisocial.xyz:
  /.well-known/did.json
  /xrpc/app.bsky.feed.describeFeedGenerator
  /xrpc/app.bsky.feed.getFeedSkeleton?feed=<feed-uri>&limit=&cursor=
  /  (human status page for all feeds)

The "repost share" rule used by several feeds is approximate: a post qualifies
only if its repost count is at least one third of its like count. Bluesky does
not provide a public save/bookmark count, so we score and filter using likes,
reposts, quotes, and replies only.

Interest dedup is currently feed-level, not user-level. A post that has already
appeared in a feed is dropped from that feed on later refreshes. That matches
how the old "For You" feed removed posts a user had already seen in that feed,
but it is NOT per-user. The API that Bluesky gives custom feeds does not carry
the requesting user's identity in getFeedSkeleton call metadata, so genuine
per-user "hide after like / repost / save" cannot be done from a plain custom
feed generator today. To do that properly, you would need either:
  - the requesting user's DID/identifier passed to the feed somehow, or
  - a server-side mapping of user -> hidden posts, keyed by a session cookie,
    authenticated request, or signed feed user identity, in a future API shape
    that Bluesky has not provided for custom feeds yet.

"Already seen in the feed" is the practical equivalent only if the feed is
consumed by one user or a small group that shares the same view of the feed.
If multiple unrelated users subscribe and you want each of them to keep seeing
top posts until they personally interact, that is not implemented here and is
flagged below as future work.
"""

import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor

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

CACHE = {"feeds": {}, "updated": 0, "scanned": 0, "members": 0, "error": None}
CACHE_LOCK = threading.Lock()

STATE_PATH = os.environ.get("FEEDGEN_STATE_PATH") or os.path.join(
    BASE, "feedgen-state.json"
)
_state = {"seen": {f["rkey"]: set() for f in CFG["feeds"]}}
_state_lock = threading.Lock()


def _state_snapshot():
    with _state_lock:
        return {k: list(v) for k, v in _state["seen"].items()}


def _state_load(snap):
    with _state_lock:
        for rkey, arr in snap.items():
            if rkey in _state["seen"]:
                _state["seen"][rkey] = set(arr)


def save_state():
    try:
        with open(STATE_PATH, "w") as fh:
            json.dump(_state_snapshot(), fh)
    except Exception:
        pass


def load_state():
    try:
        p = os.path.abspath(STATE_PATH)
        if not os.path.exists(p):
            return
        with open(p) as fh:
            _state_load(json.load(fh))
    except Exception:
        pass


def mark_seen(rkey, uri):
    if not uri:
        return
    with _state_lock:
        _state["seen"].setdefault(rkey, set()).add(uri)
    save_state()


def rpc(host, method, data=None, token=None):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "leftist-feedgen/1.0",
        }
        | ({"Authorization": f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} HTTP {e.code}: {e.read().decode()[:200]}")


def parse_time(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def select(posts, fcfg, sc, now, owner):
    """Rank posts for one feed window. Returns list of post URIs.

    Owner posts (owner.did) always pass the gates, get score * owner.boost,
    and bypass the per-author cap. Already-seen posts in this feed are skipped
    so a post does not reappear after the feed refreshes while the user has it
    open.
    """
    seen = _state["seen"].get(fcfg["rkey"], set())
    cands = []
    for p in posts:
        ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
        if not ts:
            continue
        age_h = (now - parse_time(ts)).total_seconds() / 3600
        if not (fcfg.get("min_age_hours", 0) <= age_h <= fcfg["max_age_hours"]):
            continue
        is_owner = (
            owner.get("always_include")
            and owner.get("did")
            and (p.get("author") or {}).get("did") == owner["did"]
        )
        likes = p.get("likeCount", 0)
        reposts = p.get("repostCount", 0)
        quotes = p.get("quoteCount", 0)
        replies = p.get("replyCount", 0)
        if fcfg.get("min_reposts_share"):
            if likes <= 0 or reposts < likes * fcfg["min_reposts_share"]:
                continue
        s = (
            likes * sc["w_likes"]
            + reposts * sc["w_reposts"]
            + quotes * sc["w_quotes"]
            + replies * sc["w_replies"]
        )
        if fcfg.get("ranking") == "trending" and age_h > 0:
            decay = fcfg.get("decay_factor", 0.8)
            s = s / (1 + age_h**decay)
        if is_owner:
            s = s * owner.get("boost", 1.0) + owner.get("bonus", 0.0)
        else:
            gates = []
            if (fcfg.get("min_score") or 0) > 0:
                gates.append(s >= fcfg["min_score"])
            if (fcfg.get("min_likes") or 0) > 0:
                gates.append(likes >= fcfg["min_likes"])
            if gates and not any(gates):
                continue
        uri = p.get("uri")
        if not uri or uri in seen:
            continue
        cands.append((s, age_h, p, uri))
    cands.sort(key=lambda t: t[0], reverse=True)
    cap = fcfg.get("max_per_author", 0) or 0
    out, per_author = [], {}
    owner_did = (owner or {}).get("did")
    for s, age_h, p, uri in cands:
        if len(out) >= fcfg.get("max_posts", 100):
            break
        did = (p.get("author") or {}).get("did", "?")
        if did != owner_did and cap and per_author.get(did, 0) >= cap:
            continue
        per_author[did] = per_author.get(did, 0) + 1
        out.append(uri)
        mark_seen(fcfg["rkey"], uri)
    return out


def _init_db():
    """Create SQLite cache if it doesn't exist."""
    db_path = os.path.join(BASE, "feedgen.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            uri TEXT PRIMARY KEY,
            author_did TEXT NOT NULL,
            indexed_at TEXT NOT NULL,
            like_count INTEGER DEFAULT 0,
            repost_count INTEGER DEFAULT 0,
            quote_count INTEGER DEFAULT 0,
            reply_count INTEGER DEFAULT 0,
            record_json TEXT NOT NULL,
            fetched_at REAL NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_did)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_indexed ON posts(indexed_at)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def _get_fetch_state(conn, key, default=None):
    row = conn.execute("SELECT value FROM fetch_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def _set_fetch_state(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO fetch_state VALUES (?, ?)", (key, value))
    conn.commit()


def _cache_posts(conn, posts):
    """Insert or update posts in the cache."""
    now = time.time()
    for p in posts:
        uri = p.get("uri")
        if not uri:
            continue
        ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt") or ""
        author_did = (p.get("author") or {}).get("did", "?")
        conn.execute(
            """INSERT OR REPLACE INTO posts
               (uri, author_did, indexed_at, like_count, repost_count, quote_count, reply_count, record_json, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                uri, author_did, ts,
                p.get("likeCount", 0), p.get("repostCount", 0),
                p.get("quoteCount", 0), p.get("replyCount", 0),
                json.dumps(p), now,
            ),
        )
    conn.commit()


def _count_cached_posts(conn, max_age_h, now_dt):
    """Count how many posts we have cached within the time window."""
    cutoff = (now_dt - __import__("datetime").timedelta(hours=max_age_h)).isoformat()
    row = conn.execute("SELECT COUNT(*) FROM posts WHERE indexed_at >= ?", (cutoff,)).fetchone()
    return row[0]


def refresh():
    """One member scan, then per-feed selection. Returns (picks_by_rkey, scanned, members)."""
    src, sc = CFG["source"], CFG["scoring"]
    handle = ENV.get("BSKY_HANDLE")
    password = ENV.get("BSKY_APP_PASSWORD")
    if not handle or not password:
        raise RuntimeError("missing BSKY_HANDLE/BSKY_APP_PASSWORD in feedgen .env")
    sess = rpc(src["pds_host"], "com.atproto.server.createSession",
               {"identifier": handle, "password": password})
    token = sess["accessJwt"]
    appview = src["appview_host"]
    owner_did = CFG.get("owner", {}).get("did")
    now = datetime.now(timezone.utc)

    # Enumerate all list members
    members, cursor = [], None
    while True:
        q = urllib.parse.urlencode(
            {"list": src["list_uri"], "limit": src["member_page_limit"]}
            | ({"cursor": cursor} if cursor else {}))
        d = rpc(appview, f"app.bsky.graph.getList?{q}", token=token)
        members.extend(d.get("items", []))
        cursor = d.get("cursor")
        if not cursor:
            break
    if owner_did and not any(
        (m.get("subject") or {}).get("did") == owner_did for m in members
    ):
        members.append({"subject": {"did": owner_did}})

    member_dids = [(m.get("subject") or {}).get("did") for m in members]
    member_dids = [d for d in member_dids if d]

    # Find the widest time window across all feeds
    max_window_h = max(f.get("max_age_hours", 48) for f in CFG["feeds"])

    # Init SQLite
    conn = _init_db()

    # Determine fetch strategy:
    # - If we have no cached data yet, fetch only recent posts (48h) to bootstrap quickly
    # - On subsequent runs, fetch only posts newer than our last fetch
    # - Every N refreshes, do a deeper backfill
    cached_count = _count_cached_posts(conn, max_window_h, now)
    last_fetch_ts = _get_fetch_state(conn, "last_fetch_ts")
    fetch_depth_h = 48  # default: fetch last 48h of posts

    if cached_count == 0:
        # First run: bootstrap with recent posts only (fast)
        fetch_depth_h = 48
    elif last_fetch_ts:
        # Subsequent runs: fetch since last fetch (incremental)
        fetch_depth_h = min(max_window_h, 48)  # cap at window size

    cutoff_ts = (now - __import__("datetime").timedelta(hours=fetch_depth_h)).isoformat()

    def fetch_member_recent(did):
        """Fetch recent posts from one member (lightweight, few pages)."""
        posts = []
        cursor = None
        for _ in range(10):  # max 10 pages = 1000 posts per member
            try:
                q = urllib.parse.urlencode(
                    {"actor": did, "limit": 100}
                    | ({"cursor": cursor} if cursor else {}))
                d = rpc(appview, f"app.bsky.feed.getAuthorFeed?{q}", token=token)
            except RuntimeError:
                break
            feed_items = d.get("feed", [])
            if not feed_items:
                break
            reached_cutoff = False
            for i in feed_items:
                if src["include_reposts"] or "reason" not in i:
                    p = i["post"]
                    ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
                    if ts:
                        if ts >= cutoff_ts:
                            posts.append(p)
                        else:
                            reached_cutoff = True
            if reached_cutoff:
                break
            next_cursor = d.get("cursor")
            if not next_cursor:
                break
            cursor = next_cursor
        return posts

    # Fetch recent posts for all members
    fetched_posts = []
    with ThreadPoolExecutor(max_workers=src["max_workers"]) as ex:
        for batch in ex.map(fetch_member_recent, member_dids):
            fetched_posts.extend(batch)

    # Cache the fetched posts
    _cache_posts(conn, fetched_posts)
    _set_fetch_state(conn, "last_fetch_ts", now.isoformat())

    # Read all cached posts within the widest window for scoring
    cutoff_full = (now - __import__("datetime").timedelta(hours=max_window_h)).isoformat()
    rows = conn.execute(
        "SELECT record_json FROM posts WHERE indexed_at >= ?",
        (cutoff_full,)
    ).fetchall()
    all_posts = [json.loads(r[0]) for r in rows]

    conn.close()

    # Deduplicate
    seen, uniq = set(), []
    for p in all_posts:
        if p.get("uri") and p["uri"] not in seen:
            seen.add(p["uri"])
            uniq.append(p)

    owner = CFG.get("owner", {})
    picks = {f["rkey"]: select(uniq, f, sc, now, owner) for f in CFG["feeds"]}
    return picks, len(uniq), len(members)


def refresh_loop():
    while True:
        try:
            picks, scanned, members = refresh()
            with CACHE_LOCK:
                CACHE.update(
                    feeds=picks,
                    updated=time.time(),
                    scanned=scanned,
                    members=members,
                    error=None,
                )
                state_uris = sum(len(v) for v in _state["seen"].values())
                CACHE["state_uris"] = state_uris
            total = sum(len(v) for v in picks.values())
            print(
                f"[feedgen] refresh ok: {total} picks / {scanned} scanned / state_uris={state_uris}",
                flush=True,
            )
        except Exception as e:
            with CACHE_LOCK:
                CACHE["error"] = f"{type(e).__name__}: {e}"[:300]
            print(f"[feedgen] refresh failed: {e}", flush=True)
        time.sleep(CFG.get("refresh_secs", 600))


class Handler(BaseHTTPRequestHandler):
    server_version = "leftist-feedgen/2.0"

    def _send(self, code, obj, ctype="application/json", headers=None):
        body = json.dumps(obj).encode() if isinstance(obj, (dict, list)) else obj
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/.well-known/did.json":
            return self._send(
                200,
                {
                    "@context": ["https://www.w3.org/ns/did/v1"],
                    "id": SERVICE_DID,
                    "service": [
                        {
                            "id": "#bsky_fg",
                            "type": "BskyFeedGenerator",
                            "serviceEndpoint": f"https://{HOSTNAME}",
                        }
                    ],
                },
            )
        if u.path == "/xrpc/app.bsky.feed.describeFeedGenerator":
            return self._send(
                200,
                {"did": SERVICE_DID, "feeds": [{"uri": uri} for uri in FEEDS]},
            )
        if u.path == "/xrpc/app.bsky.feed.getFeedSkeleton":
            uri = q.get("feed", [None])[0]
            if uri not in FEEDS:
                return self._send(
                    400, {"error": "UnknownFeed", "message": "unknown feed"}
                )
            try:
                limit = max(1, min(100, int(q.get("limit", ["50"])[0])))
            except ValueError:
                limit = 50
            try:
                offset = max(0, int(q.get("cursor", ["0"])[0]))
            except ValueError:
                offset = 0
            with CACHE_LOCK:
                picks = list(CACHE["feeds"].get(FEEDS[uri]["rkey"], []))
            page = picks[offset : offset + limit]
            out = {"feed": [{"post": p} for p in page]}
            if offset + limit < len(picks):
                out["cursor"] = str(offset + limit)
            return self._send(
                200,
                out,
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate",
                    "Pragma": "no-cache",
                },
            )
        if u.path in ("/", "/health"):
            with CACHE_LOCK:
                snap = {
                    k: (
                        v
                        if k != "feeds"
                        else {rk: list(posts) for rk, posts in v.items()}
                    )
                    for k, v in CACHE.items()
                }
            rows = "".join(
                f"<li><code>{f['rkey']}</code> ({f['display_name']}): "
                f"{len(snap['feeds'].get(f['rkey'], []))} posts</li>"
                for f in CFG["feeds"]
            )

            html = (
                f"<h1>Leftist top-posts feeds</h1><ul>{rows}</ul>"
                f"<p>scanned={snap['scanned']} members={snap['members']} "
                f"updated={snap['updated']:.0f} error={snap['error']}</p>"
                f"<p>state_uris={snap.get('state_uris', '?')}</p>"
            ).encode()
            return self._send(200, html, "text/html")
        return self._send(404, {"error": "NotFound"})

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    load_state()
    print("[feedgen] warming cache (first refresh)...", flush=True)
    try:
        picks, scanned, members = refresh()
        CACHE.update(feeds=picks, updated=time.time(), scanned=scanned, members=members)
        save_state()
        print(
            f"[feedgen] warm ok: {sum(len(v) for v in picks.values())} picks",
            flush=True,
        )
    except Exception as e:
        CACHE["error"] = f"warm failed: {e}"[:300]
        print(
            f"[feedgen] warm failed (serving empty until refresh): {e}",
            flush=True,
        )
    threading.Thread(target=refresh_loop, daemon=True).start()
    HTTPServer(("127.0.0.1", CFG.get("port", 8004)), Handler).serve_forever()
