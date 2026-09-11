#!/usr/bin/env python3
import json, os, urllib.request, urllib.parse
from datetime import datetime, timezone
BASE = os.path.expanduser("~/feedgen")
def load_json(p):
    with open(os.path.join(BASE,p)) as f: return json.load(f)
def load_env(p):
    vals={}
    with open(p) as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k,v=line.split("=",1)
            vals[k.strip()]=v.strip().strip("\"'")
    return vals
CFG=load_json("feedgen.json")
ENV=load_env(".env")
SRC=CFG["source"]; APPVIEW=SRC["appview_host"]; PDS=SRC["pds_host"]
def rpc(host, method, data=None, token=None):
    req=urllib.request.Request(f"{host}/xrpc/{method}",
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type":"application/json","User-Agent":"leftist-diag/1.0"}
        | ({"Authorization":f"Bearer {token}"} if token else {}),
        method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return json.load(r)
    except urllib.error.HTTPError as e:
        return None
handle=ENV.get("BSKY_HANDLE")
pw=ENV.get("BSKY_APP_PASSWORD")
if not handle or not pw:
    print("missing creds in .env"); raise SystemExit(1)
sess=rpc(PDS,"com.atproto.server.createSession",{"identifier":handle,"password":pw})
if not sess:
    print("sess FAIL"); raise SystemExit(1)
token=sess["accessJwt"]
members=[]; cursor=None
while True:
    q=urllib.parse.urlencode({"list":SRC["list_uri"],"limit":SRC["member_page_limit"]}|({"cursor":cursor} if cursor else {}))
    d=rpc(APPVIEW,f"app.bsky.graph.getList?{q}",token=token)
    if not d: print("getList FAIL"); break
    members.extend(d.get("items",[]))
    cursor=d.get("cursor")
    if not cursor: break
print("members on thinkcentre via .env auth:", len(members))
now=datetime.now(timezone.utc)
shown=0
for m in members:
    did=(m.get("subject") or {}).get("did")
    if not did: continue
    params={"actor":did,"limit":8}
    d=rpc(APPVIEW,"app.bsky.feed.getAuthorFeed",params,token)
    if not d: continue
    hits=[]
    for item in d.get("feed",[]):
        p=item.get("post",item)
        ts=p.get("indexedAt") or (p.get("record") or {}).get("createdAt")
        age=None
        if ts:
            age=(now-datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc)).total_seconds()/3600
        likes=p.get("likeCount",0); reposts=p.get("repostCount",0)
        share=reposts/likes if likes else None
        if share is not None and share>=0.33:
            hits.append((p.get("uri"),likes,reposts,round(share,3),round(age,2) if age is not None else None))
    if hits:
        print(f"{did}: {len(hits)} posts with share>=0.33")
        for uri,likes,reposts,share,age in hits[:3]:
            print(f"   {uri} | likes={likes} reposts={reposts} share={share} age_h={age}")
        shown+=1
    if shown>=6: break
