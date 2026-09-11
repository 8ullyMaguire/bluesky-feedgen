#!/usr/bin/env python3
"""Publish (or re-publish) the app.bsky.feed.generator records for every feed
in feedgen.json that is not already on-chain.

Idempotent: an existing record with the same displayName/description is left
alone unless --force is passed. Prints, per feed, what it did.

Usage:
  publish-feeds.py --dry-run
  publish-feeds.py
  publish-feeds.py --force
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))


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


CFG = json.load(open(os.path.join(BASE, "feedgen.json")))
ENV = {}
for p in (os.path.join(BASE, ".env"), os.path.expanduser("~/feedgen/.env"),
          os.path.expanduser("~/.hermes/.env")):
    ENV = load_dotenv(p) | ENV
ENV |= dict(os.environ)

PDS = CFG["source"]["pds_host"]
APPVIEW = CFG["source"]["appview_host"]
REPO = CFG["publisher_did"]
SERVICE_DID = f"did:web:{CFG['hostname']}"


def rpc(host, method, data=None, token=None, timeout=45):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", "User-Agent": "leftist-publish/1.0"}
        | ({"Authorization": f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} HTTP {e.code}: {e.read().decode()[:300]}")


def existing(token):
    out = {}
    try:
        q = urllib.parse.urlencode({"repo": REPO,
                                    "collection": "app.bsky.feed.generator",
                                    "limit": 100})
        d = rpc(PDS, f"com.atproto.repo.listRecords?{q}", token=token)
        for r in d.get("records", []):
            out[r["uri"].rsplit("/", 1)[-1]] = r.get("value") or {}
    except Exception as e:
        print(f"[publish] could not list existing records: {e}")
    return out


def main(argv):
    dry = "--dry-run" in argv
    force = "--force" in argv
    handle, password = ENV.get("BSKY_HANDLE"), ENV.get("BSKY_APP_PASSWORD")
    if not handle or not password:
        print("Missing BSKY_HANDLE/BSKY_APP_PASSWORD", file=sys.stderr)
        return 2
    sess = rpc(PDS, "com.atproto.server.createSession",
               {"identifier": handle, "password": password})
    token = sess["accessJwt"]
    have = existing(token)
    print(f"[publish] on-chain records found: {sorted(have)}")

    changed = 0
    for f in CFG["feeds"]:
        rkey = f["rkey"]
        want = {
            "$type": "app.bsky.feed.generator",
            "did": SERVICE_DID,
            "displayName": f["display_name"],
            "description": f["description"][:3000],
            "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        cur = have.get(rkey)
        if cur and not force and cur.get("displayName") == want["displayName"] \
                and cur.get("description") == want["description"]:
            print(f"  = {rkey:<18} unchanged")
            continue
        if dry:
            print(f"  DRY {'update' if cur else 'create'} {rkey:<18} "
                  f"{f['display_name']!r}")
            continue
        try:
            rpc(PDS, "com.atproto.repo.putRecord", {
                "repo": REPO, "collection": "app.bsky.feed.generator",
                "rkey": rkey, "record": want}, token)
            changed += 1
            print(f"  + {rkey:<18} {'updated' if cur else 'created'} "
                  f"{f['display_name']!r}")
        except RuntimeError as e:
            print(f"  ERR {rkey}: {e}")
        time.sleep(0.2)

    print(f"[publish] done: {changed} written, dry_run={dry}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
