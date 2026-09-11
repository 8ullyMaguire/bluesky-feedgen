#!/usr/bin/env bash
# Deploy the Leftist feed stack to thinkcentre. Idempotent; safe to re-run.
#
#   ./deploy.sh            full deploy (code + units + restart + timers)
#   ./deploy.sh code       only push code and restart the generator
#   ./deploy.sh units      only (re)install the systemd user units
#   ./deploy.sh verify     only run the post-deploy checks
set -euo pipefail

HOST="${HOST:-thinkcentre}"
REMOTE_DIR=/home/alvaro/feedgen
UNIT_DIR=/home/alvaro/.config/systemd/user
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"

say() { printf '\n=== %s\n' "$*"; }

push_code() {
  say "backup live state on $HOST"
  ssh "$HOST" "cd $REMOTE_DIR && cp -f feedgen-state.json feedgen-state.json.bak-$STAMP 2>/dev/null || true; ls -la feedgen-state.json* | tail -3"

  say "push code"
  scp -q "$LOCAL_DIR/feedgen.py" "$LOCAL_DIR/feedgen.json" \
        "$LOCAL_DIR/action-bot.py" "$LOCAL_DIR/list-updater.py" \
        "$HOST:$REMOTE_DIR/"

  say "remove the 577 MB dead sqlite cache (unreferenced by any code path)"
  ssh "$HOST" "cd $REMOTE_DIR && rm -f feedgen.db && ls -la | grep -c feedgen.db || true"

  say "restart feedgen"
  ssh "$HOST" "systemctl --user restart feedgen.service && sleep 3 && systemctl --user is-active feedgen.service"
}

push_units() {
  say "install systemd user units"
  scp -q "$LOCAL_DIR/systemd/"*.service "$LOCAL_DIR/systemd/"*.timer "$HOST:$UNIT_DIR/"
  ssh "$HOST" "systemctl --user daemon-reload && \
    systemctl --user enable --now action-bot.timer list-updater.timer && \
    systemctl --user list-timers action-bot.timer list-updater.timer --no-pager"
}

verify() {
  say "live feed counts"
  for f in leftist-4h leftist-24h leftist-3d leftist-week leftist-2w leftist-1m \
           leftist-4m leftist-trending leftist-foryou leftist-ml; do
    printf '  %-18s ' "$f"
    curl -s --max-time 25 \
      "https://leftist.polarisocial.xyz/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:bnjxoctfnukack6evxsbexc4/app.bsky.feed.generator/$f&limit=100" \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('feed',[])),'posts')" 2>/dev/null || echo "FAILED"
  done

  say "status page"
  curl -s --max-time 25 https://leftist.polarisocial.xyz/ | sed 's/<[^>]*>/ /g' | tr -s ' \n' ' '

  say "service state"
  ssh "$HOST" "systemctl --user is-active feedgen.service; \
    journalctl --user -u feedgen.service -n 8 --no-pager | tail -8"
}

case "${1:-all}" in
  code)   push_code ;;
  units)  push_units ;;
  verify) verify ;;
  all)    push_code; push_units; verify ;;
  *)      echo "usage: $0 [all|code|units|verify]" >&2; exit 1 ;;
esac
