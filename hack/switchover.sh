#!/usr/bin/env bash
# Moves the primary of a PostgresCluster to another member with Patroni's
# switchover API. Patroni only promotes a replica that is caught up, and turns
# the old primary into a replica of the new one.
#
# Usage: hack/switchover.sh [cluster] [candidate]
#   cluster    PostgresCluster name (default: pg)
#   candidate  member to promote, e.g. pg-2 (default: Patroni picks the
#              healthiest replica)
# Env: NAMESPACE (default: default), KUBECTL (default: kubectl),
#      CHECK_ONLY=1 to connect and show the cluster without switching.
set -euo pipefail

CLUSTER=${1:-pg}
CANDIDATE=${2:-}
NAMESPACE=${NAMESPACE:-default}
KUBECTL=${KUBECTL:-kubectl}

fail() { echo "switchover: $*" >&2; exit 1; }

status=$("$KUBECTL" -n "$NAMESPACE" get postgrescluster "$CLUSTER" -o json) ||
	fail "PostgresCluster $NAMESPACE/$CLUSTER not found"
primary=$(jq -r '.status.primary // ""' <<<"$status")
[ -n "$primary" ] || fail "$CLUSTER has no primary right now (phase $(jq -r '.status.phase' <<<"$status")); a switchover needs one"

member() { jq -r --arg n "$1" --arg f "$2" '.status.members[] | select(.name == $n) | .[$f] // ""' <<<"$status"; }

if [ -n "$CANDIDATE" ]; then
	[ "$CANDIDATE" != "$primary" ] || fail "$CANDIDATE is already the primary"
	[ -n "$(member "$CANDIDATE" name)" ] || fail "$CANDIDATE is not a member of $CLUSTER: $(jq -r '[.status.members[].name] | join(", ")' <<<"$status")"
fi

# Talk to Patroni from the primary itself, over its private address.
host=$(member "$primary" publicIPv4)
api="http://$(member "$primary" privateIPv4):8008"
pinned=$(member "$primary" hostKey)
[ -n "$host" ] && [ -n "$pinned" ] || fail "status has no address or host key for $primary yet"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
"$KUBECTL" -n "$NAMESPACE" get secret "$CLUSTER-ssh" -o jsonpath='{.data.ssh-privatekey}' | base64 -d >"$tmp/key"
chmod 600 "$tmp/key"

# Trust only the host key the controller pinned for this member.
ssh-keyscan -T 10 "$host" 2>/dev/null >"$tmp/scanned" || true
while read -r line; do
	case "$line" in "" | "#"*) continue ;; esac
	if [ "$(ssh-keygen -lf - <<<"$line" 2>/dev/null | awk '{print $2}')" = "$pinned" ]; then
		echo "$line" >>"$tmp/known_hosts"
	fi
done <"$tmp/scanned"
[ -s "$tmp/known_hosts" ] || fail "$primary ($host) did not present the pinned host key $pinned"

remote() {
	ssh -i "$tmp/key" -o UserKnownHostsFile="$tmp/known_hosts" -o StrictHostKeyChecking=yes \
		-o ConnectTimeout=10 -o LogLevel=ERROR "root@$host" "$@"
}
leader() { remote "curl -s --max-time 5 $api/cluster" | jq -r '.members[] | select(.role == "leader") | .name'; }

echo "Cluster $NAMESPACE/$CLUSTER: primary is $primary"
remote "curl -s --max-time 5 $api/cluster" |
	jq -r '.members[] | "  \(.name)\t\(.role)\t\(.state)\tlag=\(.lag // "-")"'

if [ "${CHECK_ONLY:-}" = 1 ]; then
	echo "CHECK_ONLY=1: not switching"
	exit 0
fi

body=$(jq -cn --arg l "$primary" --arg c "$CANDIDATE" '{leader: $l} + (if $c == "" then {} else {candidate: $c} end)')
echo "Switching over from $primary to ${CANDIDATE:-the healthiest replica}"
reply=$(remote "curl -s --max-time 60 -w '\n%{http_code}' -X POST -H 'Content-Type: application/json' -d '$body' $api/switchover")
code=$(tail -n1 <<<"$reply")
echo "Patroni: $(sed '$d' <<<"$reply")"
[ "$code" = 200 ] || fail "Patroni refused the switchover (HTTP $code)"

# Wait until Patroni reports the new leader.
for _ in $(seq 1 30); do
	now=$(leader 2>/dev/null || true)
	if [ -n "$now" ] && [ "$now" != "$primary" ]; then
		echo "Done: $now is the primary. The dashboard and 'kubectl get pgc' show it within 30 seconds."
		exit 0
	fi
	sleep 2
done
fail "Patroni accepted the switchover, but $primary is still the leader after 60 seconds"
