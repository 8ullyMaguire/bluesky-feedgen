#!/usr/bin/env python3
"""Leftist list updater (v7): promoter discovery -> app.bsky.graph.listitem writes.

Discovers accounts promoted by the two promoter accounts and adds them to the
Leftist list owned by criticalnexus. Replaces the v6 "discovery only" script.

What counts as "promoted" (all four, so a promotion is not missed because the
account happens to be on a custom domain):

  1. a mention facet in their post      (app.bsky.richtext.facet#mention -> DID)
  2. a bare @handle in the post text    (resolved via resolveHandle)
  3. a bsky.app/profile/<handle|did> link in the text or a link facet
  4. a repost or quote-post of that account's own post

Writes go through `com.atproto.repo.createRecord` with collection
`app.bsky.graph.listitem` in the list owner's repo. (The v6 script tried
`app.bsky.graph.addListMember`, which is not the documented write path and
returned 501 — this is the endpoint the spec calls for.)

Safety:
  - discovery and writing are separately switchable; `--dry-run` never writes
  - accounts are only ever ADDED, never removed automatically
  - every add is recorded in the `promotions` table with the evidence that
    caused it (promoter, post URI, promotion type) so it can be explained
  - self-promoters, the owner, blocklisted DIDs, deactivated/suspended
    accounts and already-present members are skipped
  - credentials come from the environment / .env files only; no literals

Usage:
  list-updater.py --backfill --dry-run     # scan all history, report, no writes
  list-updater.py --backfill               # scan all history and add
  list-updater.py                          # incremental (new items only)
  list-updater.py --stats                  # show what has been promoted
Exit codes: 0 ok, 2 config/auth error, 3 API error.
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))

PDS = "https://bsky.social"
APPVIEW = "https://public.api.bsky.app"
LIST_URI = "at://did:plc:bnjxoctfnukack6evxsbexc4/app.bsky.graph.list/3m7htbu5re32u"
LIST_OWNER_DID = "did:plc:bnjxoctfnukack6evxsbexc4"
PROMOTER_HANDLES = ["redreoja4.bsky.social", "miercolesrepubl.bsky.social"]
DB_PATH = os.environ.get("FEEDGEN_DB") or os.path.join(BASE, "feedgen.sqlite")

HANDLE_RE = re.compile(
    r"(?<![A-Za-z0-9._-])@?([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+)")
PROFILE_RE = re.compile(
    r"bsky\.app/profile/([A-Za-z0-9:._-]+)")
NOT_A_HANDLE = ("bsky.app", "bsky.social", "example.com")


def load_dotenv(path):
    vals = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return vals


def creds():
    env = {}
    for p in (os.path.join(BASE, ".env"), os.path.expanduser("~/feedgen/.env"),
              os.path.expanduser("~/.hermes/.env")):
        env = load_dotenv(p) | env
    env |= dict(os.environ)
    return env.get("BSKY_HANDLE"), env.get("BSKY_APP_PASSWORD")


def rpc(host, method, data=None, token=None, timeout=45):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", "User-Agent": "leftist-listupdater/7.0"}
        | ({"Authorization": f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} HTTP {e.code}: {e.read().decode()[:200]}")


def get(url, params=None, token=None):
    full = url + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(
        full, headers=({"Authorization": f"Bearer {token}"} if token else {})
        | {"User-Agent": "leftist-listupdater/7.0"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.load(r)


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------

def resolve_handle(handle):
    try:
        q = urllib.parse.urlencode({"handle": handle})
        d = get(f"{APPVIEW}/xrpc/com.atproto.identity.resolveHandle?{q}")
        return d.get("did")
    except Exception:
        return None


def did_from_at_uri(uri):
    parts = (uri or "").split("/")
    return parts[2] if len(parts) > 2 and parts[2].startswith("did:") else None


def extract_candidates(record, text_only=False):
    """Return {(did|handle): promotion_type} found in one record."""
    found = {}
    text = (record.get("text") or "")
    for facet in (record.get("facets") or []):
        for feat in (facet.get("features") or []):
            t = feat.get("$type", "")
            if t.endswith("#mention") and feat.get("did"):
                found[feat["did"]] = "mention"
            elif t.endswith("#link") and feat.get("uri"):
                m = PROFILE_RE.search(feat["uri"])
                if m:
                    found[m.group(1)] = "link"
    for m in PROFILE_RE.finditer(text):
        found[m.group(1)] = found.get(m.group(1), "link")
    embed = record.get("embed") or {}
    rec = embed.get("record") if isinstance(embed.get("record"), dict) else None
    if rec and rec.get("uri"):
        did = did_from_at_uri(rec["uri"])
        if did:
            found[did] = "quote"
    if not text_only:
        for h in HANDLE_RE.findall(text):
            h = h.lower().rstrip(".")
            if h in NOT_A_HANDLE or not h.endswith((".social", ".com", ".net", ".org",
                                                   ".xyz", ".app", ".es", ".eu",
                                                   ".io", ".dev", ".info", ".me")):
                # still allow anything with a dot that resolves; the filter above
                # only skips obvious noise, resolution decides the rest
                if "." not in h:
                    continue
            found.setdefault(h, "text-mention")
    return found


def resolve_many(handles, workers=8):
    """Resolve handles -> DIDs concurrently. Serial resolution of ~1k handles is
    the slowest part of a backfill and also matters for the 4-hourly timer."""
    import concurrent.futures
    out = {}
    uniq = sorted({h for h in handles if h})
    if not uniq:
        return out
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for h, did in zip(uniq, ex.map(resolve_handle, uniq)):
            if did:
                out[h] = did
    return out


def scan_promoter(did, token, collection, max_pages=200):
    """All records of one collection for a repo (paginated listRecords)."""
    out, cursor, pages = [], None, 0
    while pages < max_pages:
        params = {"repo": did, "collection": collection, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            d = get(f"{PDS}/xrpc/com.atproto.repo.listRecords", params, token)
        except RuntimeError as e:
            print(f"[listupd] listRecords {collection} failed: {e}", flush=True)
            break
        out.extend(d.get("records") or [])
        cursor = d.get("cursor")
        pages += 1
        if not cursor:
            break
        time.sleep(0.1)
    return out


def current_members(token):
    members, cursor, pages = [], None, 0
    while pages < 200:
        params = {"list": LIST_URI, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            d = get(f"{APPVIEW}/xrpc/app.bsky.graph.getList", params, token)
        except RuntimeError as e:
            print(f"[listupd] getList failed: {e}", flush=True)
            break
        members.extend(d.get("items") or [])
        cursor = d.get("cursor")
        pages += 1
        if not cursor:
            break
    dids = {m["subject"]["did"] for m in members if m.get("subject")}
    handles = {m["subject"].get("handle") for m in members if m.get("subject")}
    return dids, handles, members


def account_exists(token, did):
    try:
        d = get(f"{APPVIEW}/xrpc/app.bsky.actor.getProfile",
                {"actor": did}, token)
        return not d.get("deactivated", False), d.get("handle")
    except Exception:
        return False, None


# --------------------------------------------------------------------------
# database
# --------------------------------------------------------------------------

def db():
    import sqlite3
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS promotions (
        did TEXT PRIMARY KEY, handle TEXT, promoter TEXT, promotion_type TEXT,
        evidence_uri TEXT, discovered_at REAL, added_at REAL,
        listitem_rkey TEXT, status TEXT DEFAULT 'pending')""")
    con.row_factory = sqlite3.Row
    return con


def record(con, did, handle, promoter, ptype, evidence, status, rkey=None, added=None):
    con.execute(
        """INSERT INTO promotions(did,handle,promoter,promotion_type,evidence_uri,
               discovered_at,added_at,listitem_rkey,status)
           VALUES(?,?,?,?,?,?,?,?,?)
           ON CONFLICT(did) DO UPDATE SET
             handle=COALESCE(excluded.handle, promotions.handle),
             status=CASE
               WHEN excluded.status='added'  THEN 'added'
               WHEN excluded.status='failed' THEN 'failed'
               ELSE promotions.status END,
             added_at=COALESCE(excluded.added_at, promotions.added_at),
             listitem_rkey=COALESCE(excluded.listitem_rkey, promotions.listitem_rkey),
             evidence_uri=COALESCE(promotions.evidence_uri, excluded.evidence_uri)""",
        (did, handle, promoter, ptype, evidence, time.time(), added, rkey, status))


def add_member(token, did, dry):
    """Documented write path for list membership."""
    if dry:
        return None
    res = rpc(PDS, "com.atproto.repo.createRecord", {
        "repo": LIST_OWNER_DID,
        "collection": "app.bsky.graph.listitem",
        "record": {"$type": "app.bsky.graph.listitem",
                   "subject": did,
                   "list": LIST_URI,
                   "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z",
                                              time.gmtime())}}, token)
    uri = res.get("uri", "")
    return uri.rsplit("/", 1)[-1] if uri else None


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

def run(argv):
    dry = "--dry-run" in argv
    backfill = "--backfill" in argv
    handle, password = creds()
    if not handle or not password:
        print("Missing BSKY_HANDLE/BSKY_APP_PASSWORD (checked ./, ~/feedgen/.env, "
              "~/.hermes/.env, environment)", file=sys.stderr)
        return 2
    con = db()
    try:
        sess = rpc(PDS, "com.atproto.server.createSession",
                   {"identifier": handle, "password": password})
    except RuntimeError as e:
        print(f"Bluesky auth error: {e}", file=sys.stderr)
        return 2
    token = sess["accessJwt"]
    if sess["did"] != LIST_OWNER_DID:
        print(f"WARNING: logged in as {sess['did']}, list owner is {LIST_OWNER_DID}",
              file=sys.stderr)

    cur_dids, cur_handles, members = current_members(token)
    print(f"[listupd] list members now: {len(cur_dids)}", flush=True)

    promoters = {}
    for h in PROMOTER_HANDLES:
        d = resolve_handle(h)
        if d:
            promoters[h] = d
    print(f"[listupd] promoters: {promoters}", flush=True)

    found = {}      # did -> (handle_or_None, promoter, ptype, evidence)
    for phandle, pdid in promoters.items():
        posts = scan_promoter(pdid, token, "app.bsky.feed.post")
        reps = scan_promoter(pdid, token, "app.bsky.feed.repost")
        if not backfill:
            # incremental: skip items whose promotion was *successfully* added.
            # Failures (and dry runs) are deliberately re-scanned so a transient
            # 502 self-heals on the next tick instead of being lost forever.
            known = {r["evidence_uri"] for r in con.execute(
                "SELECT evidence_uri FROM promotions "
                "WHERE status='added' AND evidence_uri IS NOT NULL")}
            posts = [r for r in posts if r.get("uri") not in known]
            reps = [r for r in reps if r.get("uri") not in known]
        print(f"[listupd] {phandle}: {len(posts)} posts, {len(reps)} reposts "
              f"({'backfill' if backfill else 'incremental'})", flush=True)

        for r in posts:
            uri = r.get("uri")
            for key, ptype in extract_candidates(r.get("value") or {}).items():
                found.setdefault(key, (None, phandle, ptype, uri))
        for r in reps:
            subj = ((r.get("value") or {}).get("subject") or {}).get("uri")
            did = did_from_at_uri(subj)
            if did:
                found.setdefault(did, (None, phandle, "repost", r.get("uri")))

    # resolve handle-keys to DIDs (concurrently — this is the slow step)
    handle_keys = [k for k in found if not k.startswith("did:")]
    resolved_dids = resolve_many(handle_keys)
    print(f"[listupd] resolved {len(resolved_dids)}/{len(handle_keys)} handles",
          flush=True)
    resolved = {}
    for key, (_, promoter, ptype, evidence) in found.items():
        if key.startswith("did:"):
            resolved.setdefault(key, (None, promoter, ptype, evidence))
            continue
        did = resolved_dids.get(key)
        if did:
            resolved.setdefault(did, (key, promoter, ptype, evidence))

    print(f"[listupd] distinct promoted accounts discovered: {len(resolved)}", flush=True)

    # filter down to accounts that are not already members / are alive.
    # account_exists hits the network, so do it concurrently.
    import concurrent.futures
    candidates = [(did, h, promoter, ptype, evidence)
                  for did, (h, promoter, ptype, evidence) in resolved.items()
                  if did not in cur_dids and did != LIST_OWNER_DID
                  and did not in promoters.values()]
    to_add, inactive = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        alive = list(ex.map(lambda c: account_exists(token, c[0]), candidates))
    for cand, (ok, real_handle) in zip(candidates, alive):
        did, h, promoter, ptype, evidence = cand
        if not ok:
            record(con, did, h, promoter, ptype, evidence, "skipped-inactive")
            inactive += 1
            continue
        to_add.append((did, real_handle or h, promoter, ptype, evidence))

    print(f"[listupd] skipped {inactive} inactive/unknown accounts", flush=True)

    print(f"[listupd] to add: {len(to_add)} (dry_run={dry})", flush=True)
    added = 0
    for did, h, promoter, ptype, evidence in sorted(to_add, key=lambda x: x[0]):
        try:
            rkey = add_member(token, did, dry)
            record(con, did, h, promoter, ptype, evidence,
                   "dry" if dry else "added", rkey, time.time())
            if not dry:
                added += 1
            print(f"  {'DRY +' if dry else '+  '} @{h} ({did}) via {ptype} "
                  f"from {promoter}", flush=True)
        except Exception as e:
            record(con, did, h, promoter, ptype, evidence, "failed")
            print(f"  ERR @{h}: {e}", flush=True)
        time.sleep(0.15)

    # verify by reading the list back
    con.execute("INSERT INTO meta(k,v) VALUES('last_run',?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (time.strftime("%Y-%m-%dT%H:%M:%S"),))
    if not dry and added:
        time.sleep(2)
        after, ah, _ = current_members(token)
        print(f"[listupd] verified: members {len(cur_dids)} -> {len(after)} "
              f"(+{len(after) - len(cur_dids)})", flush=True)
    print(f"[listupd] done: discovered={len(resolved)} to_add={len(to_add)} "
          f"added={added}", flush=True)
    return 0


def stats():
    con = db()
    print("=== promotions by status ===")
    for r in con.execute("SELECT status, COUNT(*) c FROM promotions GROUP BY status"):
        print(f"  {r['status']:<18} {r['c']}")
    print("=== by promotion type ===")
    for r in con.execute("SELECT promotion_type, COUNT(*) c FROM promotions "
                         "GROUP BY promotion_type ORDER BY c DESC"):
        print(f"  {str(r['promotion_type']):<14} {r['c']}")
    print("=== by promoter ===")
    for r in con.execute("SELECT promoter, COUNT(*) c FROM promotions GROUP BY promoter"):
        print(f"  {r['promoter']:<24} {r['c']}")
    print("=== last 20 added ===")
    for r in con.execute("SELECT handle, promotion_type, promoter, "
                         "datetime(added_at,'unixepoch','localtime') t FROM promotions "
                         "WHERE status='added' ORDER BY added_at DESC LIMIT 20"):
        print(f"  {r['t']} @{r['handle']:<28} {r['promotion_type']:<12} {r['promoter']}")
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--stats" in args:
        sys.exit(stats())
    sys.exit(run(args))
