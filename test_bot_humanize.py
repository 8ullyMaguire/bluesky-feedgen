#!/usr/bin/env python3
"""Tests for the humanize + digest additions to action-bot.py.

Run on the host (or thinkcentre): python3 test_bot_humanize.py
Uses a throwaway DB and never writes to Bluesky.
"""
import importlib.util
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

TMP = tempfile.mkdtemp(prefix="bot-test-")
os.environ["FEEDGEN_DB"] = os.path.join(TMP, "t.sqlite")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ab", os.path.join(_here, "action-bot.py"))
ab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ab)  # type: ignore[union-attr]

# `posts` and `taste` are owned by feedgen.py; create the parts this test uses.
_con = ab.db()
_con.executescript("""
CREATE TABLE IF NOT EXISTS posts (
    uri TEXT PRIMARY KEY, cid TEXT, author_did TEXT NOT NULL,
    author_handle TEXT, indexed_at TEXT NOT NULL,
    like_count INTEGER DEFAULT 0, repost_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0, reply_count INTEGER DEFAULT 0,
    text TEXT DEFAULT '', langs TEXT DEFAULT '', record_json TEXT DEFAULT '{}',
    first_seen REAL NOT NULL, last_seen REAL NOT NULL);
CREATE TABLE IF NOT EXISTS taste (
    author_did TEXT PRIMARY KEY, likes_count INTEGER DEFAULT 0,
    updated_at REAL NOT NULL);
""")


def test_jitter_is_bounded_and_varies():
    ab.ACT["humanize"] = {"enabled": True, "jitter_minutes": 4}
    # patch sleep so the test does not actually wait
    slept = []
    real = ab.time.sleep
    ab.time.sleep = lambda s: slept.append(s)
    try:
        for _ in range(20):
            ab.maybe_delay()
    finally:
        ab.time.sleep = real
    assert all(0 <= s <= 240 for s in slept), slept
    assert len(set(slept)) > 5, "jitter must vary, not repeat one value"
    assert max(slept) - min(slept) > 30, (min(slept), max(slept))
    print(f"ok  jitter bounded 0..240s and varies ({min(slept):.0f}..{max(slept):.0f}s)")

    ab.ACT["humanize"] = {"enabled": False}
    assert ab.maybe_delay() == 0.0
    print("ok  jitter disabled -> no delay")


def test_skip_slot_probability_and_hour_weights():
    ab.ACT["humanize"] = {"enabled": True, "skip_slot_chance": 0.0,
                          "hour_weights": {}}
    assert sum(ab.maybe_skip_slot("t") for _ in range(50)) == 0
    print("ok  skip chance 0 -> never skips")

    ab.ACT["humanize"] = {"enabled": True, "skip_slot_chance": 1.0,
                          "hour_weights": {}}
    assert sum(ab.maybe_skip_slot("t") for _ in range(20)) == 20
    print("ok  skip chance 1.0 -> always skips")

    ab.ACT["humanize"] = {"enabled": True, "skip_slot_chance": 0.0,
                          "hour_weights": {"0": 0.0}}
    # patch the hour to one that is weighted to zero activity
    real_dt = ab.datetime
    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 9, 11, 0, 30)
    ab.datetime = FakeDT
    try:
        assert sum(ab.maybe_skip_slot("t") for _ in range(20)) == 20
    finally:
        ab.datetime = real_dt
    print("ok  hour_weights can idle an hour completely")

    ab.ACT["humanize"] = {"enabled": True, "skip_slot_chance": 0.15,
                          "hour_weights": {}}
    n = sum(ab.maybe_skip_slot("t") for _ in range(2000))
    assert 200 < n < 400, n          # ~15% of 2000
    print(f"ok  skip chance 0.15 -> {n/2000:.1%} of slots skipped")


def test_affinity_authors_mutuals_and_likes():
    con = ab.db()
    con.execute("DELETE FROM follows")
    con.execute("DELETE FROM taste")
    con.executemany(
        "INSERT INTO follows(actor_did,subject_did,handle,direction,seen_at) "
        "VALUES(?,?,?,?,?)",
        [("me", "mutual", "m.b", "follows", 0), ("mutual", "me", "me.b", "followers", 0),
         ("me", "outonly", "o.b", "follows", 0),
         ("inonly", "me", "i.b", "followers", 0)])
    con.execute("INSERT INTO taste(author_did,likes_count,updated_at) VALUES(?,?,?)",
                ("liked", 10, 0))
    aff = ab.affinity_authors(con)
    assert aff.get("mutual") == 1.0, aff
    assert aff.get("liked", 0) > 0, aff
    assert aff.get("outonly", 0) < 1.0 and aff.get("inonly", 0) < 1.0, aff
    print(f"ok  affinity: mutual=1.0, liked={aff['liked']:.2f}, "
          f"one-way={aff.get('outonly')}")


def test_digest_selection_one_per_author_and_scoring():
    con = ab.db()
    con.execute("DELETE FROM posts")
    now = datetime.now(timezone.utc)
    rows = []
    for i, (did, likes, reposts, age_h) in enumerate([
            ("a", 100, 50, 2),      # best
            ("a", 90, 40, 3),       # same author -> must be skipped
            ("b", 80, 30, 5),
            ("c", 70, 20, 8),
            ("d", 60, 90, 10),      # repost-heavy, could outscore others
            ("e", 50, 10, 12),
            ("f", 40, 5, 1),        # low score
            ("g", 30, 2, 30)]):     # outside the 24h window
        ts = (now - timedelta(hours=age_h)).isoformat().replace("+00:00", "Z")
        rows.append((f"at://{did}/app.bsky.feed.post/{i}", "cid", did, f"{did}.b",
                     ts, likes, reposts, 0, 0,
                     f"this is a real sentence about politics number {i} and it "
                     f"keeps going with more words so it passes the prose filter",
                     0, time.time()))
    con.executemany(
        "INSERT OR REPLACE INTO posts(uri,cid,author_did,author_handle,indexed_at,"
        "like_count,repost_count,quote_count,reply_count,text,first_seen,last_seen) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    mc = {"top_n": 5, "max_age_hours": 24, "min_likes": 15}
    picks = ab.digest_candidates(con, mc)
    authors = [p["handle"] for p in picks]
    assert len(picks) == 5, picks
    assert len(set(authors)) == 5, authors          # one post per author
    assert "a.b" in authors
    assert not any(p["handle"] == "g.b" for p in picks), "30h old must be excluded"
    scores = [p["score"] for p in picks]
    assert scores == sorted(scores, reverse=True), scores
    print(f"ok  digest: {len(picks)} posts, unique authors, window+score honoured")

    # emoji/one-word spam must not qualify, however popular
    con.execute("INSERT OR REPLACE INTO posts(uri,cid,author_did,author_handle,"
                "indexed_at,like_count,repost_count,quote_count,reply_count,text,"
                "first_seen,last_seen) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("at://spam/app.bsky.feed.post/1", "cid", "spam", "spam.b",
                 now.isoformat().replace("+00:00", "Z"), 9999, 9999, 0, 0,
                 "👏👏👏👏👏👏👏👏👏👏👏👏", 0, time.time()))
    picks2 = ab.digest_candidates(con, mc)
    assert not any(p["handle"] == "spam.b" for p in picks2), \
        "emoji-only post with huge engagement must be filtered out"
    print("ok  digest rejects emoji-only/one-word spam despite high engagement")

    mc_hi = {"top_n": 5, "max_age_hours": 24, "min_likes": 5000}
    assert ab.digest_candidates(con, mc_hi) == []
    print("ok  digest with unreachable min_likes -> no candidates")


def test_facets_use_byte_offsets_with_unicode():
    text = "Top ☭ de hoy\nhttps://bsky.app/profile/x/post/1\nmás"
    url = "https://bsky.app/profile/x/post/1"
    f = ab.build_facets(text, [(url, url)])
    assert len(f) == 1, f
    bs, be = f[0]["index"]["byteStart"], f[0]["index"]["byteEnd"]
    assert text.encode("utf-8")[bs:be].decode("utf-8") == url, (bs, be)
    assert f[0]["features"][0]["$type"].endswith("#link")
    # char offsets would have been wrong because of the multibyte prefix
    assert bs != text.find(url), "byte offset must differ from char offset here"
    print(f"ok  facets: byte offsets correct with multibyte text (bs={bs})")


def test_digest_not_due_outside_hour():
    con = ab.db()
    con.execute("DELETE FROM digests")
    mc = {"per_day": 1, "post_at_hour": 23}
    real_dt = ab.datetime
    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 9, 11, 9, 0)
    ab.datetime = FakeDT
    try:
        lines = ab.do_digest(con, "faketoken", "me", mc, [])
    finally:
        ab.datetime = real_dt
    assert lines == [], lines
    note = con.execute("SELECT note FROM bot_runs WHERE mode='digest' "
                       "ORDER BY id DESC LIMIT 1").fetchone()["note"]
    assert note == "not due", note
    print("ok  digest not posted before its hour")


def test_dry_digest_does_not_consume_daily_quota():
    con = ab.db()
    con.execute("DELETE FROM digests")
    con.execute("DELETE FROM bot_runs WHERE mode='digest'")
    # a dry row from earlier today must not block the real post
    con.execute("INSERT INTO digests(at,status,uris,detail) VALUES(?,?,?,?)",
                (time.time(), "dry", "[]", "earlier dry run"))
    mc = {"per_day": 1, "post_at_hour": 0, "top_n": 5, "max_age_hours": 24,
          "min_likes": 0, "min_text_chars": 0, "min_words": 0}
    picks = ab.digest_candidates(con, mc)
    assert picks, "fixture posts from the previous test should still qualify"
    # dry=True forces no Bluesky write; assert it still reports due
    lines = ab.do_digest(con, "faketoken", "me", dict(mc, dry_run=True), ["--dry-run"])
    assert lines and lines[0].startswith("DRY digest"), lines
    real = con.execute("SELECT COUNT(*) c FROM digests WHERE status='ok'").fetchone()["c"]
    assert real == 0, "dry run must not create an 'ok' digest"
    print("ok  dry digest does not consume the daily quota")


def test_fit_text_respects_the_grapheme_cap():
    long = "a" * 400
    out = ab.fit_text(long)
    assert len(out) <= 295, len(out)
    assert out.endswith("…")
    # emoji/ZWJ sequences are the risky case; keeping codepoints under the cap is
    # always safe because a grapheme is >= 1 codepoint
    emoji = "👨‍👩‍👧‍👦" * 100
    assert len(ab.fit_text(emoji)) <= 295
    assert ab.fit_text("short") == "short"
    print("ok  fit_text keeps every post under Bluesky's 300-grapheme cap")


def test_digest_entries_fit_and_carry_facets():
    """The digest must survive the real API limits, which is what broke the first
    live attempt (1189 graphemes > 300)."""
    import json as _json
    raw = _json.load(open(os.path.join(_here, "feedgen.json")))
    title = raw["actions"]["digest"]["title"]
    sub = raw["actions"]["digest"]["subtitle"]
    root = ab.fit_text(f"{title}\n\n{sub}")
    assert len(root) <= 295, len(root)
    # a worst-case entry: long handle, long snippet
    url = "https://bsky.app/profile/some.long.handle.bsky.social/post/3mv6uth3af222"
    entry = ab.fit_text(f"1) ❤ 723 · 🔁 460 · @some.long.handle.bsky.social\n"
                        f"{url}\n" + "palabra " * 40)
    assert len(entry) <= 295, len(entry)
    f = ab.build_facets(entry, [(url, url)])
    assert len(f) == 1, f
    bs, be = f[0]["index"]["byteStart"], f[0]["index"]["byteEnd"]
    assert entry.encode("utf-8")[bs:be].decode("utf-8") == url
    print("ok  digest root + entries fit 300 graphemes and facets stay valid")


def test_repost_cycle_and_saved_configs_present():
    # read the file fresh: earlier tests mutate ab.ACT["humanize"] on purpose
    raw = json.load(open(os.path.join(_here, "feedgen.json")))
    a = raw["actions"]
    rc, sr, dg = a["repost_cycle"], a["saved_reposts"], a["digest"]
    assert len(rc["posts"]) == 4, rc["posts"]
    assert all(p.startswith("at://did:plc:") for p in rc["posts"])
    assert rc["interval_hours"] == 4 and rc["window_end_hour"] == 24
    assert sr["per_day"] == 3 and sr["exclude_pinned"] is True
    assert dg["top_n"] == 5 and dg["per_day"] == 1
    assert a["repost"]["author_cooldown_days"] == 7
    assert a["humanize"]["stop_markers"] == ["#stopbot"]
    assert a["humanize"]["jitter_minutes"] == 4
    assert 0 < a["humanize"]["skip_slot_chance"] < 1
    print("ok  config: 4 pinned posts, 4h cycle, 3 saved/day, digest 5/day-hour, "
          "7-day cooldown, #stopbot marker, jitter + skip configured")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
    print("\nALL BOT TESTS PASSED")
