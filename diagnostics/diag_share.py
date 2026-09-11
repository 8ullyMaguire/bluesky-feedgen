import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone
BASE = os.path.dirname(os.path.abspath(__file__))
def load_json(path):
    with open(os.path.join(BASE, path)) as f:
        return json.load(f)
CFG = load_json("feedgen.json")
SRC = CFG["source"]
APPVIEW = SRC["appview_host"]
PDS = SRC["pds_host"]
def rpc(host, method, data=None, token=None):
    req = urllib.request.Request(
        f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json", "User-Agent": "leftist-diag/1.0"}
        | ({"Authorization": f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return None
handle = os.environ.get("BSKY_HANDLE", "criticalnexus.bsky.social")
pw = os.environ.get("BSKY_APP_PASSWORD")
if not pw:
    print("missing BSKY_APP_PASSWORD"); raise SystemExit(1)
sess = rpc(PDS, "com.atproto.server.createSession", {"identifier": handle, "password": pw})
if not sess:
    print("session FAIL"); raise SystemExit(1)
token = sess["accessJwt"]
members, cursor = [], None
while True:
    q = urllib.parse.urlencode(
        {"list": SRC["list_uri"], "limit": SRC["member_page_limit"]}
        | ({"cursor": cursor} if cursor else {}))
    d = rpc(APPVIEW, f"app.bsky.graph.getList?{q}", token=token)
    if not d:
        print("getList FAIL"); break
    members.extend(d.get("items", []))
    cursor = d.get("cursor")
    if not cursor:
        break
print("members:", len(members))
now = datetime.now(timezone.utc)
shown = 0
for m in members:
    did = (m.get("subject") or {}).get("did")
    if not did:
        continue
    d = rpc(APPVIEW, f"app.bsky.feed.getAuthorFeed?{urllib.parse.urlencode({'actor': did, 'limit': 8})}", token=token)
    if not d:
        print("feed FAIL", did); continue
    hits = []
    for item in d.get("feed", []):
        p = item.get("post", item)
        ts = p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
        age = None
        if ts:
            age = (now - datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)).total_seconds() / 3600
        likes = p.get("likeCount", 0)
        reposts = p.get("repostCount", 0)
        share = reposts / likes if likes else None
        if share is not None and share >= 0.33:
            hits.append((p.get("uri"), likes, reposts, round(share, 3), round(age, 2) if age is not None else None, round(p.get("likeCount",0)*1.0 + p.get("repostCount",0)*3.0,1)))
    if hits:
        print(f"{did}: {len(hits)} posts with share>=0.33")
        for uri, likes, reposts, share, age, score in hits[:4]:
            print(f"   {uri} | likes={likes} reposts={reposts} share={share} age_h={age} score={score}")
        shown += 1
    if shown >= 6:
        break
