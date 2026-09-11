#!/usr/bin/env python3
"""Tests for feedgen v2: scoring, per-feed overrides, gates, dedup TTL.

Run: python3 test_feedgen.py
Uses a throwaway SQLite file so the real feedgen.sqlite is untouched.
"""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

TMP = tempfile.mkdtemp(prefix="feedgen-test-")
os.environ["FEEDGEN_DB"] = os.path.join(TMP, "test.sqlite")
os.environ["FEEDGEN_STATE_PATH"] = os.path.join(TMP, "no-legacy.json")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import feedgen  # noqa: E402

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
OWNER = {"did": "did:plc:owner", "always_include": True, "boost": 10.0, "bonus": 100.0}
CTX = {"ml_affinity": {}, "ml_seeds": set(), "taste": {}, "ml_keywords": []}


def post(age_h, likes=10, reposts=5, quotes=1, replies=1, did="did:plc:a",
         uri=None, text=""):
    ts = (NOW - timedelta(hours=age_h)).isoformat().replace("+00:00", "Z")
    return {
        "uri": uri or f"at://{did}/app.bsky.feed.post/{age_h}-{likes}-{reposts}",
        "indexedAt": ts,
        "likeCount": likes, "repostCount": reposts,
        "quoteCount": quotes, "replyCount": replies,
        "author": {"did": did},
        "record": {"createdAt": ts, "text": text, "langs": ["en"]},
    }


def fcfg(**kw):
    base = {"rkey": "t", "min_age_hours": 0, "max_age_hours": 2880,
            "min_score": 0, "min_likes": 0, "min_reposts_share": 0,
            "max_per_author": 100, "max_posts": 100, "seen_ttl_hours": 0}
    base.update(kw)
    return base


def test_base_score_uses_weights():
    p = post(1, likes=10, reposts=2, quotes=3, replies=4)
    w = {"w_likes": 1.0, "w_reposts": 3.0, "w_quotes": 2.0, "w_replies": 0.5}
    assert feedgen.base_score(p, w) == 10 + 6 + 6 + 2, feedgen.base_score(p, w)
    w2 = {"w_likes": 2.0, "w_reposts": 1.0, "w_quotes": 0.0, "w_replies": 0.0}
    assert feedgen.base_score(p, w2) == 22
    # saves are structurally zero — an explicit weight must still change nothing
    w3 = dict(w2, w_saves=99.0)
    assert feedgen.base_score(p, w3) == feedgen.base_score(p, w2)
    print("ok  base_score + weights + w_saves is a no-op")


def test_weights_for_overrides_per_feed():
    cfg, f = {"scoring": {"w_likes": 1.0, "w_reposts": 3.0}}, {"scoring": {"w_reposts": 10.0}}
    old = feedgen.CFG
    feedgen.CFG = cfg
    try:
        assert feedgen.weights_for(f)["w_reposts"] == 10.0
        assert feedgen.weights_for(f)["w_likes"] == 1.0
        assert feedgen.weights_for({})["w_reposts"] == 3.0
    finally:
        feedgen.CFG = old
    print("ok  per-feed scoring override wins over the global block")


def test_gate_mode_all_not_any():
    # 5 likes, 50 likes gate fails; min_score 0 disabled -> must be excluded
    posts = [post(1, likes=5, reposts=5, uri="at://low")]
    uris, st = feedgen.select(posts, fcfg(min_likes=50, min_score=1), NOW, OWNER, CTX, set())
    assert uris == [], uris
    # old buggy any() would have let it through on the score gate
    posts = [post(1, likes=50, reposts=20, uri="at://hi")]
    uris, _ = feedgen.select(posts, fcfg(min_likes=50, min_score=1), NOW, OWNER, CTX, set())
    assert uris == ["at://hi"], uris
    print("ok  gates use AND semantics (any -> all fixed)")


def test_owner_bypasses_gates_and_suppression():
    posts = [post(1, likes=0, reposts=0, did="did:plc:owner", uri="at://owner-post")]
    f = fcfg(min_likes=100, min_reposts_share=0.33)
    uris, st = feedgen.select(posts, f, NOW, OWNER, CTX, {"at://owner-post"})
    assert uris == ["at://owner-post"], uris
    assert st["owner"] == 1
    print("ok  owner passes gates + suppression window")


def test_ttl_suppression_filters_and_releases():
    posts = [post(1, likes=10, reposts=5, uri="at://p1")]
    f = fcfg(seen_ttl_hours=24)
    uris, st = feedgen.select(posts, f, NOW, OWNER, CTX, {"at://p1"})
    assert uris == [] and st["suppressed"] == 1, (uris, st)
    # once outside the suppression window it comes back (caller passes empty set)
    uris, _ = feedgen.select(posts, f, NOW, OWNER, CTX, set())
    assert uris == ["at://p1"], uris
    print("ok  bounded suppression hides then releases (no permanent drain)")


def test_dedupe_by_score_then_uri():
    posts = [post(1, likes=10, reposts=5, uri="at://b", did="did:plc:x"),
             post(1, likes=10, reposts=5, uri="at://a", did="did:plc:y")]
    uris, _ = feedgen.select(posts, fcfg(), NOW, OWNER, CTX, set())
    assert uris == ["at://a", "at://b"], uris
    print("ok  ordering is deterministic (score desc, uri asc)")


def test_per_author_cap_and_max_posts():
    posts = [post(1, likes=10, reposts=5, did="did:plc:same",
                  uri=f"at://s{i}") for i in range(10)]
    uris, _ = feedgen.select(posts, fcfg(max_per_author=3), NOW, OWNER, CTX, set())
    assert len(uris) == 3, uris
    uris, _ = feedgen.select(posts, fcfg(max_per_author=100, max_posts=4),
                             NOW, OWNER, CTX, set())
    assert len(uris) == 4
    print("ok  per-author cap and max_posts enforced")


def test_trending_and_deep_ranking_differ():
    # Hand-computed with decay 0.8 (trending, denominator 1+age^d) and 0.6 (deep,
    # denominator age^d with an age floor):
    #   trending: recent 25/1.5743=15.88   older 75/4.0312=18.61 -> older first
    #   deep:     recent 25/0.6598=37.89   older 75/2.2974=32.65 -> recent first
    recent = post(0.5, likes=10, reposts=5, uri="at://recent")     # base 25
    older = post(4, likes=25, reposts=15, uri="at://older")        # base 70..75
    older["likeCount"], older["repostCount"] = 25, 16              # base 73 -> 75 w/ quote+reply
    tr = fcfg(ranking="trending", decay_factor=0.8)
    dp = fcfg(ranking="deep", decay_factor=0.6, age_floor_hours=0.25)
    uris_tr, _ = feedgen.select([recent, older], tr, NOW, OWNER, CTX, set())
    uris_dp, _ = feedgen.select([recent, older], dp, NOW, OWNER, CTX, set())
    assert uris_tr != uris_dp, (uris_tr, uris_dp)
    assert uris_tr[0] == "at://older", uris_tr
    assert uris_dp[0] == "at://recent", uris_dp
    print("ok  trending (1+age^d) and deep (age^d, floored) rank differently")


def test_ranking_formulas_match_spec():
    p = post(4, likes=10, reposts=5, quotes=1, replies=1)   # base = 10+15+2+0.5
    w = feedgen.weights_for({})
    base = feedgen.base_score(p, w)
    ctx = {"ml_affinity": {}, "ml_seeds": set(), "taste": {}, "ml_keywords": []}
    assert abs(feedgen.rank_score(p, fcfg(ranking="trending", decay_factor=0.8),
                                 w, 4.0, ctx) - base / (1 + 4.0 ** 0.8)) < 1e-9
    assert abs(feedgen.rank_score(p, fcfg(ranking="deep", decay_factor=0.6),
                                 w, 4.0, ctx) - base / (4.0 ** 0.6)) < 1e-9
    # deep's age floor stops a brand-new post from being multiplied to infinity
    assert abs(feedgen.rank_score(p, fcfg(ranking="deep", decay_factor=0.6,
                                         age_floor_hours=0.25), w, 0.0, ctx)
               - base / (0.25 ** 0.6)) < 1e-9
    print("ok  trending/deep formulas match their documented spec")


def test_ml_tilt_moves_ml_author_up():
    plain = post(2, likes=30, reposts=15, did="did:plc:plain", uri="at://plain")
    ml = post(2, likes=10, reposts=5, did="did:plc:ml", uri="at://ml",
              text="materialismo dialéctico y lucha de clases")
    f = fcfg(ranking="ml", ml_weight=2.5, ml_keyword_bonus=0.35, decay_factor=0.6)
    ctx = dict(CTX, ml_affinity={"did:plc:ml": 1.0},
               ml_keywords=["materialismo dialéctico", "lucha de clases"])
    uris, _ = feedgen.select([plain, ml], f, NOW, OWNER, ctx, set())
    assert uris[0] == "at://ml", uris
    print("ok  ML affinity + keyword bonus lift an ML post above a bigger plain post")


def test_sqlite_schema_and_state_pruning():
    con = feedgen.db()
    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("posts", "seen", "actions", "promotions", "author_ml", "taste", "meta"):
        assert t in tables, (t, tables)
    import time as _t
    con.execute("INSERT OR REPLACE INTO seen(rkey,uri,shown_at,shown_count) "
                "VALUES('t','at://old',?,1)", (_t.time() - 60 * 86400,))
    con.execute("INSERT OR REPLACE INTO seen(rkey,uri,shown_at,shown_count) "
                "VALUES('t','at://new',?,1)", (_t.time(),))
    feedgen.prune_state()
    left = {r[0] for r in con.execute("SELECT uri FROM seen")}
    assert "at://old" not in left and "at://new" in left, left
    print("ok  sqlite schema present; prune drops stale suppression rows")


def test_feed_config_is_consistent():
    keys = [f["rkey"] for f in feedgen.CFG["feeds"]]
    assert len(keys) == len(set(keys)), "duplicate rkey"
    for f in feedgen.CFG["feeds"]:
        assert f["max_age_hours"] > 0
        assert f.get("seen_ttl_hours", 24) >= 0
        assert f.get("ranking", "flat") in ("flat", "trending", "deep", "foryou", "ml")
        feedgen.weights_for(f)  # must not raise
    a = feedgen.CFG["actions"]
    assert a["repost"]["window_end_hour"] == 24
    assert a["repost"]["per_hour"] == 3
    assert a["repost"]["per_day"] is None
    print(f"ok  {len(keys)} feed configs valid: {', '.join(keys)}")


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
    print("\nALL TESTS PASSED")
