#!/usr/bin/env python3
"""Bluesky action bot: scheduled auto-repost + auto-like from the Leftist setup.

Reads the `actions` block of feedgen.json and the same .env / feedgen.sqlite as
the feed generator, so there is one config and one store.

Design decisions (see the vault spec, W4/W5):

  - Both modes act from the main account (`actions.account`). Reposts run live
    with no daily cap by explicit decision; the throttle is the per_hour
    cadence inside the window (3/hour, 08:00-24:00 = 48/day). Likes default to
    dry_run until reviewed.
  - The cadence is *cumulative-target*: at any moment the bot asks "how many
    actions should have happened since the window opened?" and does the
    difference. A missed tick self-corrects on the next one; nothing catches up
    in a burst beyond `max_per_run`.
  - Candidates come from a *rotating slice* of the follow graph, because
    fetching 900+ author feeds every 10 minutes is not polite to the AppView.
  - Every action is idempotent: `actions` has UNIQUE(kind, uri) and a
    pre-write existence check. Re-running never double-likes or double-reposts.
  - Opt-out markers, replies, reposts, sensitive labels, self, age window and
    author cooldowns are all respected. `panic: true` stops every write.

Usage:
  action-bot.py                 # honour the config (respects dry_run)
  action-bot.py --dry-run       # force dry-run for both modes
  action-bot.py --live          # force live for both modes
  action-bot.py --report        # print recent actions and exit
  action-bot.py --mode repost   # only one mode
Exit codes: 0 ok, 2 config/auth error, 3 API error.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

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
ACT = CFG.get("actions", {})
ENV = load_dotenv(".env") | dict(os.environ)
DB_PATH = os.environ.get("FEEDGEN_DB") or os.path.join(BASE, "feedgen.sqlite")
PDS = CFG["source"]["pds_host"]
APPVIEW = CFG["source"]["appview_host"]
OWNER_DID = CFG.get("owner", {}).get("did")
REPORT = os.path.join(BASE, "actions-report.md")

import sqlite3  # noqa: E402  (kept after path/config setup for readability)


def db():
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.row_factory = sqlite3.Row
    ensure_schema(con)
    return con


REQUIRED_TABLES = """
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
CREATE TABLE IF NOT EXISTS follows (
    actor_did   TEXT NOT NULL,
    subject_did TEXT NOT NULL,
    handle      TEXT,
    direction   TEXT NOT NULL,
    seen_at     REAL NOT NULL,
    PRIMARY KEY (actor_did, subject_did, direction)
);
CREATE TABLE IF NOT EXISTS bot_runs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    mode       TEXT NOT NULL,
    started    REAL NOT NULL,
    finished   REAL,
    candidates INTEGER DEFAULT 0,
    acted      INTEGER DEFAULT 0,
    dry_run    INTEGER DEFAULT 1,
    note       TEXT
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS repost_cycle (
    uri     TEXT PRIMARY KEY,
    last_at REAL,
    rkey    TEXT,
    cycles  INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cycles (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    uri    TEXT NOT NULL,
    at     REAL NOT NULL,
    action TEXT NOT NULL,
    rkey   TEXT,
    status TEXT,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS saved_log (
    uri    TEXT PRIMARY KEY,
    at     REAL,
    status TEXT,
    detail TEXT
);
"""


def ensure_schema(con):
    """Self-healing: the bot must work even if it runs before feedgen ever
    created the database (the timer can fire independently)."""
    con.executescript(REQUIRED_TABLES)


def rpc(host, method, data=None, token=None, timeout=30):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", "User-Agent": "leftist-actionbot/1.0"}
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
# follow graph (cached, refreshed daily)
# --------------------------------------------------------------------------

def refresh_follows(con, token, max_pages=30):
    now = time.time()
    last = con.execute("SELECT v FROM meta WHERE k='follows_refreshed'").fetchone()
    if last and now - float(last["v"]) < 86400:
        return
    rows = []
    for direction, method in (("follows", "app.bsky.graph.getFollows"),
                              ("followers", "app.bsky.graph.getFollowers")):
        cursor, pages = None, 0
        while pages < max_pages:
            q = urllib.parse.urlencode(
                {"actor": OWNER_DID, "limit": 100}
                | ({"cursor": cursor} if cursor else {}))
            try:
                d = rpc(APPVIEW, f"{method}?{q}", token=token, timeout=45)
            except RuntimeError as e:
                print(f"[bot] {method} failed: {e}", flush=True)
                break
            items = d.get(direction, []) or []
            for it in items:
                if it.get("did") and it.get("did") != OWNER_DID:
                    rows.append((OWNER_DID, it["did"], it.get("handle"),
                                 direction, now))
            cursor = d.get("cursor")
            pages += 1
            if not cursor:
                break
    if rows:
        con.executemany(
            """INSERT INTO follows(actor_did,subject_did,handle,direction,seen_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(actor_did,subject_did,direction) DO UPDATE SET
                 handle=excluded.handle, seen_at=excluded.seen_at""", rows)
        con.execute("INSERT INTO meta(k,v) VALUES('follows_refreshed',?) "
                    "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(now),))
    print(f"[bot] follow graph: {len(rows)} entries", flush=True)


def candidate_authors(con, source, batch, rotate_key):
    """A rotating slice of the follow graph, so each run scans a bounded set."""
    dirs = {"follows": ["follows"], "followers": ["followers"],
            "follows+followers": ["follows", "followers"]}.get(source, ["follows"])
    qmarks = ",".join("?" for _ in dirs)
    rows = con.execute(
        f"SELECT DISTINCT subject_did, handle FROM follows "
        f"WHERE direction IN ({qmarks}) ORDER BY subject_did", dirs).fetchall()
    if not rows:
        return []
    cur = con.execute("SELECT v FROM meta WHERE k=?", (rotate_key,)).fetchone()
    offset = int(float(cur["v"])) if cur and cur["v"] else 0
    offset %= len(rows)
    picked = [rows[(offset + i) % len(rows)] for i in range(min(batch, len(rows)))]
    con.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (rotate_key, str((offset + len(picked)) % len(rows))))
    return [(r["subject_did"], r["handle"]) for r in picked]


def fetch_author_posts(appview, did, limit=10):
    try:
        q = urllib.parse.urlencode({"actor": did, "limit": limit,
                                    "filter": "posts_no_replies"})
        d = rpc(appview, f"app.bsky.feed.getAuthorFeed?{q}", timeout=30)
        return (d.get("feed") or [])
    except Exception:
        return []


# --------------------------------------------------------------------------
# cadence
# --------------------------------------------------------------------------

def window_bounds(mode_cfg, now=None):
    now = now or datetime.now()
    start_h = int(mode_cfg.get("window_start_hour", 0))
    end_h = int(mode_cfg.get("window_end_hour", 24))
    start = now.replace(hour=start_h % 24, minute=0, second=0, microsecond=0)
    if end_h >= 24:
        end = start + timedelta(hours=(24 - start_h))
    else:
        end = now.replace(hour=end_h % 24, minute=0, second=0, microsecond=0)
        if end_h <= start_h:
            end += timedelta(days=1)
    return start, end


def due_count(mode_cfg, done_last_hour, done_today, now=None):
    """How many actions may run right now (sliding-hour cadence).

    Deliberately NOT "how many should have happened since the window opened":
    that makes a missed tick self-correct as a burst. The rate is the contract
    (3/hour means at most 3 in any rolling hour), so we look at the last hour
    and top up the difference, bounded by `max_per_run`.
    """
    now = now or datetime.now()
    start, end = window_bounds(mode_cfg, now)
    if now < start or now > end:
        return 0
    per_hour = float(mode_cfg.get("per_hour", 0) or 0)
    if per_hour <= 0:
        return 0
    room = int(per_hour) - done_last_hour
    per_day = mode_cfg.get("per_day")
    if per_day is not None:
        room = min(room, int(per_day) - done_today)
    return max(0, min(room, int(mode_cfg.get("max_per_run", 1))))


def done_recent(con, kind, mode_cfg, now=None):
    """(actions in the last rolling hour inside the window, actions since
    window open today)."""
    now = now or datetime.now()
    start, _ = window_bounds(mode_cfg, now)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    since = max(start.timestamp(), day_start, (now - timedelta(hours=1)).timestamp())
    hour_ago = max((now - timedelta(hours=1)).timestamp(), start.timestamp())
    last_hour = con.execute(
        "SELECT COUNT(*) c FROM actions WHERE kind=? AND status='ok' AND created_at>=?",
        (kind, hour_ago)).fetchone()["c"]
    today = con.execute(
        "SELECT COUNT(*) c FROM actions WHERE kind=? AND status='ok' AND created_at>=?",
        (kind, since)).fetchone()["c"]
    return last_hour, today


# --------------------------------------------------------------------------
# candidate selection
# --------------------------------------------------------------------------

def blocked_authors(con, kind, mode_cfg, now_ts):
    """Authors we must not touch again right now."""
    out = set()
    cooldown_days = mode_cfg.get("author_cooldown_days")
    per_day = mode_cfg.get("max_per_author_per_day", 0) or 0
    if cooldown_days:
        rows = con.execute(
            "SELECT author_did, MAX(created_at) m FROM actions "
            "WHERE kind=? AND status='ok' GROUP BY author_did", (kind,)).fetchall()
        for r in rows:
            if r["author_did"] and now_ts - r["m"] < cooldown_days * 86400:
                out.add(r["author_did"])
    if per_day:
        day_start = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        rows = con.execute(
            "SELECT author_did, COUNT(*) c FROM actions WHERE kind=? AND status='ok' "
            "AND created_at>=? GROUP BY author_did", (kind, day_start)).fetchall()
        for r in rows:
            if r["author_did"] and r["c"] >= per_day:
                out.add(r["author_did"])
    return out


def already_acted(con, kind):
    return {r["uri"] for r in con.execute(
        "SELECT uri FROM actions WHERE kind=?", (kind,))}


def own_repost_subjects(token):
    """Posts the owner reposted manually — never repost those again."""
    out = set()
    try:
        q = urllib.parse.urlencode({"actor": OWNER_DID, "limit": 100,
                                    "filter": "posts_and_author_threads"})
        d = rpc(APPVIEW, f"app.bsky.feed.getAuthorFeed?{q}", token=token, timeout=45)
        for it in d.get("feed", []):
            reason = it.get("reason") or {}
            if reason.get("$type", "").endswith("reasonRepost"):
                uri = ((reason.get("subject") or {}).get("uri")
                       or (it.get("post") or {}).get("uri"))
                if uri:
                    out.add(uri)
    except Exception as e:
        print(f"[bot] own-repost guard unavailable: {e}", flush=True)
    return out


def is_sensitive(p):
    rec = p.get("record") or {}
    labels = (rec.get("labels") or {}).get("values") or []
    if labels:
        return True
    for lab in (p.get("labels") or []):
        if lab.get("val") in ("porn", "sexual", "graphic-media", "nudity"):
            return True
    return False


def opt_out(text, markers):
    low = (text or "").lower()
    return any(m.lower() in low for m in markers)


def score_post(p, mode_cfg, weights):
    likes = p.get("likeCount", 0)
    reposts = p.get("repostCount", 0)
    quotes = p.get("quoteCount", 0)
    replies = p.get("replyCount", 0)
    s = (likes * weights.get("w_likes", 1.0) + reposts * weights.get("w_reposts", 3.0)
         + quotes * weights.get("w_quotes", 2.0) + replies * weights.get("w_replies", 0.5))
    ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
    if not ts:
        return None, None
    try:
        age_h = (datetime.now(timezone.utc) - parse_time(ts)).total_seconds() / 3600
    except (ValueError, TypeError):
        return None, None
    if not (mode_cfg.get("min_age_hours", 0) <= age_h <= mode_cfg.get("max_age_hours", 99999)):
        return None, None
    ranking = mode_cfg.get("ranking", "flat")
    if ranking in ("deep", "trending"):
        decay = mode_cfg.get("decay_factor", 0.6)
        if ranking == "trending":
            s = s / (1 + age_h ** decay) if age_h > 0 else s
        else:
            s = s / (max(age_h, 0.25) ** decay)
    return s, age_h


def collect_candidates(con, token, kind, mode_cfg):
    weights = dict(CFG["scoring"])
    weights.update(mode_cfg.get("scoring") or {})
    markers = ACT.get("opt_out_markers", [])
    batch = int(mode_cfg.get("batch_size", 120))
    authors = candidate_authors(con, mode_cfg.get("source", "follows"), batch,
                                f"rotate_{kind}")
    blocked = blocked_authors(con, kind, mode_cfg, time.time())
    acted = already_acted(con, kind)
    if kind == "repost":
        acted |= own_repost_subjects(token)

    posts = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for feed in ex.map(lambda a: fetch_author_posts(APPVIEW, a[0], 10), authors):
            posts.extend(feed)

    out = []
    for it in posts:
        if "reason" in it and mode_cfg.get("skip_reposts", True):
            continue
        p = it.get("post") or {}
        uri = p.get("uri")
        did = (p.get("author") or {}).get("did")
        if not uri or not did or did == OWNER_DID:
            continue
        rec = p.get("record") if isinstance(p.get("record"), dict) else {}
        if mode_cfg.get("skip_replies", True) and rec.get("reply"):
            continue
        if mode_cfg.get("skip_sensitive", True) and is_sensitive(p):
            continue
        if opt_out(rec.get("text", ""), markers):
            continue
        langs = rec.get("langs") or []
        want = mode_cfg.get("langs")
        if want and langs and not (set(langs) & set(want)):
            continue
        if did in blocked or uri in acted:
            continue
        s, age_h = score_post(p, mode_cfg, weights)
        if s is None or age_h is None:
            continue
        if s < (mode_cfg.get("min_score", 0) or 0):
            continue
        out.append({"uri": uri, "cid": p.get("cid"), "did": did,
                    "handle": (p.get("author") or {}).get("handle"),
                    "score": round(s, 2), "age_h": round(age_h, 2),
                    "likes": p.get("likeCount", 0), "reposts": p.get("repostCount", 0),
                    "text": (rec.get("text") or "")[:160]})
    out.sort(key=lambda r: (-r["score"], r["uri"]))
    return out


# --------------------------------------------------------------------------
# acting
# --------------------------------------------------------------------------

def author_day_counts(con, kind):
    """How many successful actions each author already got today."""
    day_start = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp()
    return {r["author_did"]: r["c"] for r in con.execute(
        "SELECT author_did, COUNT(*) c FROM actions WHERE kind=? AND status='ok' "
        "AND created_at>=? AND author_did IS NOT NULL GROUP BY author_did",
        (kind, day_start)).fetchall()}


def act(con, token, did_self, kind, rows, mode_cfg, dry, max_per_run):
    coll = "app.bsky.feed.like" if kind == "like" else "app.bsky.feed.repost"
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    per_author = int(mode_cfg.get("max_per_author_per_day", 0) or 0)
    counts = author_day_counts(con, kind)      # today's totals from the DB
    ran = {}                                   # this-run totals, to enforce
    acted = 0                                  # the cap *within* one run too
    lines = []
    for r in rows:
        if acted >= max_per_run:
            break
        did = r["did"]
        if per_author and counts.get(did, 0) + ran.get(did, 0) >= per_author:
            continue
        if dry:
            lines.append(f"DRY {kind} [{r['score']}] @{r['handle']} "
                         f"L{r['likes']}/R{r['reposts']} {r['age_h']}h :: {r['text'][:90]}")
            con.execute(
                "INSERT OR IGNORE INTO actions(kind,uri,cid,author_did,created_at,"
                "status,detail) VALUES(?,?,?,?,?,?,?)",
                (kind + ":dry", r["uri"], r["cid"], did, time.time(),
                 "dry", r["text"][:120]))
            ran[did] = ran.get(did, 0) + 1
            acted += 1
            continue
        try:
            res = rpc(PDS, "com.atproto.repo.createRecord", {
                "repo": did_self, "collection": coll,
                "record": {"$type": coll,
                           "subject": {"uri": r["uri"], "cid": r["cid"]},
                           "createdAt": now_iso}}, token)
            con.execute(
                "INSERT OR IGNORE INTO actions(kind,uri,cid,author_did,created_at,"
                "status,detail) VALUES(?,?,?,?,?,?,?)",
                (kind, r["uri"], r["cid"], did, time.time(), "ok",
                 res.get("uri", "")))
            acted += 1
            ran[did] = ran.get(did, 0) + 1
            lines.append(f"OK  {kind} [{r['score']}] @{r['handle']} "
                         f"L{r['likes']}/R{r['reposts']} {r['age_h']}h :: {r['text'][:90]}")
        except Exception as e:
            con.execute(
                "INSERT OR IGNORE INTO actions(kind,uri,cid,author_did,created_at,"
                "status,detail) VALUES(?,?,?,?,?,?,?)",
                (kind, r["uri"], r["cid"], did, time.time(), "fail", str(e)[:200]))
            lines.append(f"ERR {kind} @{r['handle']}: {e}")
        time.sleep(0.4)
    return acted, lines


# --------------------------------------------------------------------------
# pinned "keep circulating" reposts (repost_cycle)
# --------------------------------------------------------------------------

def fetch_post_refs(token, uris):
    """Fresh {uri: cid} for a set of posts.

    A repost's subject needs the *current* cid, so it is re-fetched every cycle
    rather than stored once.
    """
    out = {}
    uris = [u for u in uris if u]
    for i in range(0, len(uris), 25):
        chunk = uris[i:i + 25]
        try:
            q = urllib.parse.urlencode([("uris", u) for u in chunk])
            d = rpc(APPVIEW, f"app.bsky.feed.getPosts?{q}", token=token, timeout=30)
            for p in d.get("posts", []):
                if p.get("uri") and p.get("cid"):
                    out[p["uri"]] = {"cid": p["cid"],
                                     "handle": (p.get("author") or {}).get("handle"),
                                     "text": ((p.get("record") or {}).get("text") or "")[:90]}
        except Exception as e:
            print(f"[bot] getPosts failed for {len(chunk)} uris: {e}", flush=True)
    return out


def unrepost(token, did_self, rkey):
    """Delete our own repost record. Best effort: a missing record is fine."""
    try:
        rpc(PDS, "com.atproto.repo.deleteRecord", {
            "repo": did_self, "collection": "app.bsky.feed.repost", "rkey": rkey},
            token)
        return True, ""
    except Exception as e:
        return False, str(e)[:160]


def make_repost(token, did_self, uri, cid):
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    res = rpc(PDS, "com.atproto.repo.createRecord", {
        "repo": did_self, "collection": "app.bsky.feed.repost",
        "record": {"$type": "app.bsky.feed.repost",
                   "subject": {"uri": uri, "cid": cid},
                   "createdAt": now_iso}}, token)
    new_uri = res.get("uri", "")
    return new_uri.rsplit("/", 1)[-1] if new_uri else None


def do_repost_cycle(con, token, did_self, mc, args):
    """Re-repost each pinned post once per interval_hours, inside the window."""
    dry = bool(mc.get("dry_run", True))
    if "--dry-run" in args:
        dry = True
    if "--live" in args:
        dry = False
    now = datetime.now()
    start, end = window_bounds(mc, now)
    posts = mc.get("posts") or []
    interval = float(mc.get("interval_hours", 4)) * 3600
    in_window = start <= now <= end
    print(f"[bot] repost_cycle: {len(posts)} pinned, interval={mc.get('interval_hours')}h "
          f"window {start:%H:%M}-{end:%H:%M} in_window={in_window} dry={dry}",
          flush=True)
    if not in_window:
        con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                    "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                    ("repost_cycle", time.time(), time.time(), len(posts), 0,
                     int(dry), "outside window"))
        return []

    refs = fetch_post_refs(token, posts)
    missing = [u for u in posts if u not in refs]
    for u in missing:
        # a deleted or unfindable pinned post must be reported, not retried forever
        con.execute("INSERT INTO cycles(uri,at,action,rkey,status,detail) "
                    "VALUES(?,?,?,?,?,?)", (u, time.time(), "skip", None, "missing",
                                            "getPosts returned nothing"))
    if missing:
        print(f"[bot] repost_cycle: {len(missing)} pinned post(s) could not be "
              f"fetched: {missing}", flush=True)

    due = []
    for uri in posts:
        if uri not in refs:
            continue
        row = con.execute("SELECT last_at FROM repost_cycle WHERE uri=?",
                          (uri,)).fetchone()
        last = row["last_at"] if row else None
        if last is None or (time.time() - last) >= interval:
            due.append(uri)
    due = due[:int(mc.get("max_per_run", len(posts)) or len(posts))]
    print(f"[bot] repost_cycle: {len(due)} due", flush=True)

    lines, acted = [], 0
    for uri in due:
        info = refs[uri]
        row = con.execute("SELECT rkey FROM repost_cycle WHERE uri=?",
                          (uri,)).fetchone()
        old_rkey = row["rkey"] if row else None
        label = f"@{info['handle']} :: {info['text'][:70]}"
        if dry:
            lines.append(f"DRY cycle repost {uri.rsplit('/', 1)[-1]} {label}")
            continue
        # Create first, delete the previous one only after the create succeeds:
        # if the create fails the post stays reposted rather than dropping out.
        try:
            new_rkey = make_repost(token, did_self, uri, info["cid"])
        except Exception as e:
            con.execute("INSERT INTO cycles(uri,at,action,rkey,status,detail) "
                        "VALUES(?,?,?,?,?,?)",
                        (uri, time.time(), "create", None, "fail", str(e)[:160]))
            lines.append(f"ERR cycle repost {uri.rsplit('/', 1)[-1]}: {e}")
            continue
        con.execute("INSERT INTO cycles(uri,at,action,rkey,status,detail) "
                    "VALUES(?,?,?,?,?,?)",
                    (uri, time.time(), "create", new_rkey, "ok", label))
        if mc.get("mode", "refresh") == "refresh" and old_rkey and old_rkey != new_rkey:
            ok, err = unrepost(token, did_self, old_rkey)
            con.execute("INSERT INTO cycles(uri,at,action,rkey,status,detail) "
                        "VALUES(?,?,?,?,?,?)",
                        (uri, time.time(), "delete", old_rkey,
                         "ok" if ok else "fail", err))
        con.execute(
            "INSERT INTO repost_cycle(uri,last_at,rkey,cycles) VALUES(?,?,?,1) "
            "ON CONFLICT(uri) DO UPDATE SET last_at=excluded.last_at, "
            "rkey=excluded.rkey, cycles=repost_cycle.cycles+1",
            (uri, time.time(), new_rkey))
        acted += 1
        lines.append(f"OK  cycle repost {uri.rsplit('/', 1)[-1]} {label}")
        time.sleep(0.5)

    con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                ("repost_cycle", time.time(), time.time(), len(posts), acted,
                 int(dry), "due=%d" % len(due)))
    return lines


# --------------------------------------------------------------------------
# random reposts from the account's own bookmarks (saved_reposts)
# --------------------------------------------------------------------------

def fetch_bookmarks(token, max_pages=10):
    out, cursor, pages = [], None, 0
    while pages < max_pages:
        p = {"limit": 100}
        if cursor:
            p["cursor"] = cursor
        try:
            d = rpc(PDS, "app.bsky.bookmark.getBookmarks?"
                    + urllib.parse.urlencode(p), token=token, timeout=45)
        except RuntimeError as e:
            print(f"[bot] bookmarks unavailable: {e}", flush=True)
            break
        items = d.get("bookmarks") or []
        if not items:
            break
        for b in items:
            subj = b.get("subject") or {}
            it = b.get("item") or {}
            uri = subj.get("uri") or it.get("uri")
            cid = subj.get("cid") or it.get("cid")
            if uri and cid and not str(it.get("$type", "")).endswith("notFound"):
                out.append({"uri": uri, "cid": cid,
                            "handle": (it.get("author") or {}).get("handle"),
                            "text": ((it.get("record") or {}).get("text") or "")[:90]})
        cursor = d.get("cursor")
        pages += 1
        if not cursor:
            break
    return out


def do_saved_reposts(con, token, did_self, mc, args):
    """Repost `per_day` random bookmarked posts, spread across the window."""
    import random
    dry = bool(mc.get("dry_run", True))
    if "--dry-run" in args:
        dry = True
    if "--live" in args:
        dry = False
    now = datetime.now()
    start, end = window_bounds(mc, now)
    per_day = int(mc.get("per_day", 3) or 0)
    in_window = start <= now <= end
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    done_today = con.execute(
        "SELECT COUNT(*) c FROM saved_log WHERE status='ok' AND at>=?",
        (day_start,)).fetchone()["c"]
    # spread the day's quota across the window instead of dumping it at 08:00
    window_h = max((end - start).total_seconds() / 3600.0, 0.01)
    elapsed_h = max((now - start).total_seconds() / 3600.0, 0.0)
    target = int(per_day * min(elapsed_h / window_h, 1.0)) if in_window else 0
    due = max(0, min(target - done_today, int(mc.get("max_per_run", 1))))
    print(f"[bot] saved_reposts: {done_today}/{per_day} done today, target={target} "
          f"due={due} in_window={in_window} dry={dry}", flush=True)
    if not in_window or due <= 0 or per_day <= 0:
        con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                    "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                    ("saved_reposts", time.time(), time.time(), 0, 0, int(dry),
                     "nothing due"))
        return []

    bookmarks = fetch_bookmarks(token)
    pinned = set((ACT.get("repost_cycle") or {}).get("posts") or [])
    exclude_pinned = mc.get("exclude_pinned", True)
    already = {r["uri"] for r in con.execute("SELECT uri FROM saved_log")}
    acted_uris = {r["uri"] for r in con.execute(
        "SELECT uri FROM actions WHERE kind LIKE 'repost%' AND status='ok'")}
    own_reposts = own_repost_subjects(token)

    pool = []
    for b in bookmarks:
        uri = b["uri"]
        if uri in already or uri in acted_uris or uri in own_reposts:
            continue
        if exclude_pinned and uri in pinned:
            continue
        if b.get("handle") is None:
            continue
        pool.append(b)
    print(f"[bot] saved_reposts: {len(bookmarks)} bookmarks, {len(pool)} eligible",
          flush=True)
    if not pool:
        con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                    "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                    ("saved_reposts", time.time(), time.time(), 0, 0, int(dry),
                     "pool empty"))
        return []

    picks = random.sample(pool, min(due, len(pool)))
    lines, acted = [], 0
    for b in picks:
        label = f"@{b['handle']} :: {b['text'][:70]}"
        if dry:
            lines.append(f"DRY saved repost {b['uri'].rsplit('/', 1)[-1]} {label}")
            continue
        try:
            fresh = fetch_post_refs(token, [b["uri"]]).get(b["uri"])
            if not fresh:
                con.execute("INSERT INTO saved_log(uri,at,status,detail) "
                            "VALUES(?,?,?,?)", (b["uri"], time.time(), "missing",
                                                "getPosts returned nothing"))
                continue
            rkey = make_repost(token, did_self, b["uri"], fresh["cid"])
            con.execute("INSERT INTO saved_log(uri,at,status,detail) "
                        "VALUES(?,?,?,?)", (b["uri"], time.time(), "ok", rkey or ""))
            acted += 1
            lines.append(f"OK  saved repost {b['uri'].rsplit('/', 1)[-1]} {label}")
        except Exception as e:
            con.execute("INSERT INTO saved_log(uri,at,status,detail) "
                        "VALUES(?,?,?,?) ON CONFLICT(uri) DO UPDATE SET "
                        "at=excluded.at, status=excluded.status, detail=excluded.detail",
                        (b["uri"], time.time(), "fail", str(e)[:160]))
            lines.append(f"ERR saved repost {b['uri'].rsplit('/', 1)[-1]}: {e}")
        time.sleep(0.5)

    con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                ("saved_reposts", time.time(), time.time(), len(pool), acted,
                 int(dry), "due=%d" % due))
    return lines


# --------------------------------------------------------------------------
# report / main
# --------------------------------------------------------------------------

def show_report(lines=25):
    con = db()
    print("=== recent actions ===")
    for r in con.execute(
            "SELECT kind,status,author_did,datetime(created_at,'unixepoch','localtime') t,"
            "substr(detail,1,60) d FROM actions ORDER BY id DESC LIMIT ?", (lines,)):
        print(f"  {r['kind']:<11} {r['status']:<5} {r['t']} {r['d']}")
    print("=== counts by kind/status ===")
    for r in con.execute("SELECT kind,status,COUNT(*) c FROM actions GROUP BY kind,status"):
        print(f"  {r['kind']:<11} {r['status']:<5} {r['c']}")
    print("=== pinned repost cycle ===")
    for r in con.execute("SELECT uri, cycles, datetime(last_at,'unixepoch','localtime') t, "
                         "rkey FROM repost_cycle ORDER BY last_at"):
        print(f"  {r['uri'].rsplit('/',1)[-1]:<14} cycles={r['cycles']:<3} last={r['t']}")
    print("=== saved-post reposts ===")
    for r in con.execute("SELECT uri, status, datetime(at,'unixepoch','localtime') t "
                         "FROM saved_log ORDER BY at DESC LIMIT 10"):
        print(f"  {r['t']} {r['status']:<8} {r['uri'].rsplit('/',1)[-1]}")
    n_saved_today = con.execute(
        "SELECT COUNT(*) c FROM saved_log WHERE status='ok' AND at>=?",
        (datetime.now().replace(hour=0, minute=0, second=0,
                                microsecond=0).timestamp(),)).fetchone()["c"]
    print(f"  saved reposts today: {n_saved_today}"
          f"/{(ACT.get('saved_reposts') or {}).get('per_day', '?')}")
    print("=== recent cycle log ===")
    for r in con.execute("SELECT datetime(at,'unixepoch','localtime') t, action, status, "
                         "substr(detail,1,50) d FROM cycles ORDER BY id DESC LIMIT 8"):
        print(f"  {r['t']} {r['action']:<7} {r['status']:<5} {r['d']}")
    if os.path.exists(REPORT):
        print(f"\nfull report: {REPORT}")
    return 0


def main(argv):
    args = set(argv)
    if "--report" in args:
        return show_report()
    if ACT.get("panic"):
        print("[bot] panic switch is on — no actions taken", flush=True)
        return 0
    if not ACT.get("enabled", True):
        print("[bot] actions disabled in config", flush=True)
        return 0

    handle = ENV.get("BSKY_HANDLE")
    password = ENV.get("BSKY_APP_PASSWORD")
    if not handle or not password:
        print("Missing BSKY_HANDLE/BSKY_APP_PASSWORD in .env", file=sys.stderr)
        return 2

    only = None
    for a in argv:
        if a.startswith("--mode="):
            only = a.split("=", 1)[1]
    if "--mode" in argv:
        i = argv.index("--mode")
        only = argv[i + 1] if i + 1 < len(argv) else None

    con = db()
    try:
        sess = rpc(PDS, "com.atproto.server.createSession",
                   {"identifier": handle, "password": password})
    except RuntimeError as e:
        print(f"Bluesky auth error: {e}", file=sys.stderr)
        return 2
    token, did_self = sess["accessJwt"], sess["did"]
    try:
        refresh_follows(con, token)
    except Exception as e:
        print(f"[bot] follow refresh error: {e}", flush=True)

    all_lines = []
    # pinned "keep circulating" reposts and random saved-post reposts run first:
    # they are explicit instructions, not best-effort picks from the follow graph.
    for name, fn in (("repost_cycle", do_repost_cycle),
                     ("saved_reposts", do_saved_reposts)):
        mc = ACT.get(name) or {}
        if not mc.get("enabled"):
            continue
        if only and only not in (name, "repost", "all"):
            continue
        try:
            all_lines.extend(fn(con, token, did_self, mc, args))
        except Exception as e:
            print(f"[bot] {name} failed: {e}", flush=True)
            all_lines.append(f"ERR {name}: {e}")

    for kind in ("repost", "like"):
        mc = ACT.get(kind) or {}
        if not mc.get("enabled"):
            continue
        if only and kind != only:
            continue
        dry = bool(mc.get("dry_run", True))
        if "--dry-run" in args:
            dry = True
        if "--live" in args:
            dry = False
        last_hour, today = done_recent(con, kind, mc)
        due = due_count(mc, last_hour, today)
        max_per_run = int(mc.get("max_per_run", 1))
        start, end = window_bounds(mc)
        print(f"[bot] {kind}: window {start:%H:%M}-{end:%H:%M} last_hour={last_hour} "
              f"today={today} due={due} dry={dry} (limit {mc.get('per_hour')}/h, "
              f"per_day={mc.get('per_day')})", flush=True)
        if due <= 0:
            con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,"
                        "acted,dry_run,note) VALUES(?,?,?,?,?,?,?)",
                        (kind, time.time(), time.time(), 0, 0, int(dry), "nothing due"))
            continue
        cands = collect_candidates(con, token, kind, mc)
        print(f"[bot] {kind}: {len(cands)} candidates from "
              f"{int(mc.get('batch_size', 120))} authors", flush=True)
        acted, lines = act(con, token, did_self, kind, cands, mc, dry,
                           min(due, max_per_run))
        all_lines.extend(lines)
        con.execute("INSERT INTO bot_runs(mode,started,finished,candidates,acted,"
                    "dry_run,note) VALUES(?,?,?,?,?,?,?)",
                    (kind, time.time(), time.time(), len(cands), acted, int(dry),
                     "due=%d" % due))

    if all_lines:
        with open(REPORT, "a") as fh:
            fh.write(f"\n## {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            for line in all_lines:
                fh.write(f"- {line}\n")
        for line in all_lines:
            print(f"[bot] {line}", flush=True)
    else:
        print("[bot] nothing to do", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
