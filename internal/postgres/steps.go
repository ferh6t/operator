/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// StepState is how far a step got on one member.
type StepState string

const (
	// StepDone means the script finished successfully.
	StepDone StepState = "Done"
	// StepRunning means the script is running, or was just started.
	StepRunning StepState = "Running"
	// StepFailed means the last run failed. It has been started again.
	StepFailed StepState = "Failed"
)

// StepResult is the outcome of checking a step on one member.
type StepResult struct {
	State StepState
	// Log is the end of the step's log when it failed.
	Log string
}

// commandTimeout bounds every single remote command.
const commandTimeout = 60 * time.Second

// RunStep makes sure a script runs to completion on a member, without
// waiting for it. The script runs as a transient systemd unit named
// operator-<name>, so a long install does not block the reconcile. Call it
// again to check progress. A changed script runs again.
func RunStep(ctx context.Context, ex Executor, node *Node, name, script string) (StepResult, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	sum := sha256.Sum256([]byte(script))
	hash := hex.EncodeToString(sum[:])
	dir := "/var/lib/operator"
	logFile := "/var/log/operator/" + name + ".log"
	unit := "operator-" + name

	check := fmt.Sprintf(`mkdir -p -m 0700 %[1]s /var/log/operator
if [ "$(cat %[1]s/%[2]s.done 2>/dev/null)" = "%[3]s" ]; then echo done; exit 0; fi
if systemctl is-active --quiet %[4]s; then echo running; exit 0; fi
if [ "$(cat %[1]s/%[2]s.started 2>/dev/null)" = "%[3]s" ]; then echo failed; tail -n 15 %[5]s 2>/dev/null; exit 0; fi
echo new
`, dir, name, hash, unit, logFile)
	out, err := ex.Run(ctx, node, check, nil)
	if err != nil {
		return StepResult{}, err
	}
	state, log, _ := strings.Cut(out, "\n")
	switch strings.TrimSpace(state) {
	case "done":
		return StepResult{State: StepDone}, nil
	case "running":
		return StepResult{State: StepRunning}, nil
	}

	// New, changed or failed: (re)start it. The script arrives on stdin.
	start := fmt.Sprintf(`set -e
umask 077
cat > %[1]s/%[2]s.sh
echo %[3]s > %[1]s/%[2]s.started
systemctl reset-failed %[4]s 2>/dev/null || true
systemd-run --unit=%[4]s --collect --quiet /bin/bash -c 'bash %[1]s/%[2]s.sh >> %[5]s 2>&1 && echo %[3]s > %[1]s/%[2]s.done'
`, dir, name, hash, unit, logFile)
	if _, err := ex.Run(ctx, node, start, []byte(script)); err != nil {
		return StepResult{}, err
	}
	if strings.TrimSpace(state) == "failed" {
		return StepResult{State: StepFailed, Log: strings.TrimSpace(log)}, nil
	}
	return StepResult{State: StepRunning}, nil
}

// EtcdHealth asks, from node, whether each member's etcd is healthy, and
// returns the answer by member name. An error means node could not be reached.
func EtcdHealth(ctx context.Context, ex Executor, node *Node, c Cluster) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	// etcdctl exits non-zero when any member is down; the output still says which.
	cmd := fmt.Sprintf("ETCDCTL_API=3 etcdctl --endpoints=%s --command-timeout=5s endpoint health 2>&1 || true", EtcdEndpoints(c))
	out, err := ex.Run(ctx, node, cmd, nil)
	if err != nil {
		return nil, err
	}
	// No line about any endpoint means etcdctl itself failed, e.g. on a
	// Droplet that is still installing. Ask another member instead.
	if !strings.Contains(out, " is healthy") && !strings.Contains(out, " is unhealthy") {
		return nil, fmt.Errorf("etcdctl on %s gave no health answer: %s", node.Name, lastLine(out))
	}
	return ParseEtcdHealth(out, c), nil
}

// ParseEtcdHealth reads the output of `etcdctl endpoint health`. A member
// counts as healthy only when a line says "<its endpoint> is healthy".
func ParseEtcdHealth(out string, c Cluster) map[string]bool {
	health := make(map[string]bool, len(c.Nodes))
	for _, m := range c.Nodes {
		healthy := fmt.Sprintf("http://%s:%d is healthy", m.PrivateIP, EtcdClientPort)
		health[m.Name] = false
		for line := range strings.SplitSeq(out, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), healthy) {
				health[m.Name] = true
				break
			}
		}
	}
	return health
}

// HasQuorum reports whether more than half of the etcd members are healthy.
func HasQuorum(health map[string]bool) bool {
	up := 0
	for _, ok := range health {
		if ok {
			up++
		}
	}
	return up > len(health)/2
}

// StartPatroni enables and starts Patroni on a member. It does nothing when
// Patroni is already running.
func StartPatroni(ctx context.Context, ex Executor, node *Node) error {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	_, err := ex.Run(ctx, node, "systemctl enable --now patroni", nil)
	return err
}

// StartEtcd starts etcd on a member when it is not running. A member that
// tried to join before it was added to the cluster exits, so this runs on
// every reconcile.
func StartEtcd(ctx context.Context, ex Executor, node *Node) error {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	_, err := ex.Run(ctx, node, "systemctl is-active --quiet etcd || systemctl start --no-block etcd", nil)
	return err
}

// MemberStatus asks a member's own Patroni for its role and state. Unlike
// GET /cluster, which repeats what is stored in etcd and goes stale when
// etcd has no quorum, this is what the member itself is doing now.
func MemberStatus(ctx context.Context, ex Executor, node *Node) (PatroniMember, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cmd := fmt.Sprintf("curl -s --max-time 5 http://%s:%d/patroni", node.PrivateIP, PatroniAPIPort)
	out, err := ex.Run(ctx, node, cmd, nil)
	if err != nil {
		return PatroniMember{}, err
	}
	return ParseMemberStatus(node.Name, []byte(out))
}

// ParseMemberStatus parses the body of Patroni's GET /patroni. Patroni
// answers it with 503 when PostgreSQL is not running, with the same body.
func ParseMemberStatus(name string, body []byte) (PatroniMember, error) {
	var resp struct {
		State    string `json:"state"`
		Role     string `json:"role"`
		Timeline int64  `json:"timeline"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.State == "" {
		return PatroniMember{}, fmt.Errorf("parsing Patroni status of %s: no answer", name)
	}
	return PatroniMember{Name: name, Role: resp.Role, State: resp.State, Timeline: resp.Timeline}, nil
}

// PatroniMember is one member as reported by the Patroni REST API.
type PatroniMember struct {
	Name     string
	Role     string
	State    string
	Timeline int64
	// Lag is nil for the leader, or when Patroni does not know it.
	Lag *int64
}

// ClusterStatus asks Patroni on node for the state of every member.
func ClusterStatus(ctx context.Context, ex Executor, node *Node) ([]PatroniMember, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cmd := fmt.Sprintf("curl -sf --max-time 5 http://%s:%d/cluster", node.PrivateIP, PatroniAPIPort)
	out, err := ex.Run(ctx, node, cmd, nil)
	if err != nil {
		return nil, err
	}
	return ParseClusterStatus([]byte(out))
}

// ParseClusterStatus parses the body of Patroni's GET /cluster.
func ParseClusterStatus(body []byte) ([]PatroniMember, error) {
	var resp struct {
		Members []struct {
			Name     string          `json:"name"`
			Role     string          `json:"role"`
			State    string          `json:"state"`
			Timeline int64           `json:"timeline"`
			Lag      json.RawMessage `json:"lag"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing Patroni cluster status: %w", err)
	}
	members := make([]PatroniMember, 0, len(resp.Members))
	for _, m := range resp.Members {
		pm := PatroniMember{Name: m.Name, Role: m.Role, State: m.State, Timeline: m.Timeline}
		// lag is a number, or the string "unknown".
		var lag int64
		if len(m.Lag) > 0 && json.Unmarshal(m.Lag, &lag) == nil {
			pm.Lag = &lag
		}
		members = append(members, pm)
	}
	return members, nil
}

// IsLeader reports whether a Patroni role accepts writes.
func IsLeader(role string) bool {
	return role == "leader" || role == "master" || role == "primary"
}

// IsHealthy reports whether a Patroni member state means PostgreSQL is up.
func IsHealthy(state string) bool {
	return state == "running" || state == "streaming"
}

// lastLine returns the last non-empty line of s.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

// Switchover asks Patroni, through via's REST API, to move the primary from
// leader to candidate. Patroni refuses a candidate that is not caught up.
func Switchover(ctx context.Context, ex Executor, via *Node, leader, candidate string) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]string{"leader": leader, "candidate": candidate})
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("curl -s --max-time 60 -w '\\n%%{http_code}' -X POST -H 'Content-Type: application/json' -d '%s' http://%s:%d/switchover",
		body, via.PrivateIP, PatroniAPIPort)
	out, err := ex.Run(ctx, via, cmd, nil)
	if err != nil {
		return err
	}
	// curl prints the HTTP status code on the last line.
	out = strings.TrimSpace(out)
	msg, code := "", out
	if i := strings.LastIndex(out, "\n"); i >= 0 {
		msg, code = out[:i], out[i+1:]
	}
	if code != "200" {
		return fmt.Errorf("patroni refused the switchover from %s to %s (HTTP %s): %s", leader, candidate, code, msg)
	}
	return nil
}
