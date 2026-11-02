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
	"slices"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"sigs.k8s.io/yaml"
)

func testCluster() Cluster {
	return Cluster{
		Name:    "pg",
		Token:   "uid-123",
		Version: "16",
		Nodes: []Node{
			{Name: "pg-0", PrivateIP: "10.0.0.1"},
			{Name: "pg-1", PrivateIP: "10.0.0.2"},
			{Name: "pg-2", PrivateIP: "10.0.0.3"},
		},
		SuperuserPassword:   "su-pass",
		ReplicationPassword: "repl-pass",
	}
}

func TestEtcdEnvListsEveryPeer(t *testing.T) {
	c := testCluster()
	env := EtcdEnv(c, c.Nodes[1])
	for _, want := range []string{
		"ETCD_NAME=pg-1\n",
		"ETCD_LISTEN_PEER_URLS=http://10.0.0.2:2380\n",
		"ETCD_ADVERTISE_CLIENT_URLS=http://10.0.0.2:2379\n",
		"ETCD_INITIAL_CLUSTER=pg-0=http://10.0.0.1:2380,pg-1=http://10.0.0.2:2380,pg-2=http://10.0.0.3:2380\n",
		"ETCD_INITIAL_CLUSTER_TOKEN=uid-123\n",
	} {
		if !strings.Contains(env, want) {
			t.Errorf("etcd env is missing %q:\n%s", want, env)
		}
	}
}

func TestPatroniConfig(t *testing.T) {
	c := testCluster()
	out, err := PatroniConfig(c, c.Nodes[0])
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Scope string `json:"scope"`
		Name  string `json:"name"`
		Etcd3 struct {
			Hosts []string `json:"hosts"`
		} `json:"etcd3"`
		Postgresql struct {
			ConnectAddress string   `json:"connect_address"`
			DataDir        string   `json:"data_dir"`
			BinDir         string   `json:"bin_dir"`
			PgHBA          []string `json:"pg_hba"`
			Authentication struct {
				Superuser struct {
					Password string `json:"password"`
				} `json:"superuser"`
			} `json:"authentication"`
		} `json:"postgresql"`
	}
	if err := yaml.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("Patroni config is not valid YAML: %v\n%s", err, out)
	}
	if cfg.Scope != "pg" || cfg.Name != "pg-0" {
		t.Errorf("scope/name = %q/%q", cfg.Scope, cfg.Name)
	}
	if want := []string{"10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"}; !slices.Equal(cfg.Etcd3.Hosts, want) {
		t.Errorf("etcd3.hosts = %v, want %v", cfg.Etcd3.Hosts, want)
	}
	if cfg.Postgresql.ConnectAddress != "10.0.0.1:5432" {
		t.Errorf("connect_address = %q", cfg.Postgresql.ConnectAddress)
	}
	if cfg.Postgresql.DataDir != "/var/lib/postgresql/16/patroni" || cfg.Postgresql.BinDir != "/usr/lib/postgresql/16/bin" {
		t.Errorf("data_dir/bin_dir = %q/%q", cfg.Postgresql.DataDir, cfg.Postgresql.BinDir)
	}
	if cfg.Postgresql.Authentication.Superuser.Password != "su-pass" {
		t.Errorf("superuser password not rendered")
	}
	if !slices.Contains(cfg.Postgresql.PgHBA, "host replication replicator 10.0.0.3/32 scram-sha-256") {
		t.Errorf("pg_hba does not allow replication from pg-2: %v", cfg.Postgresql.PgHBA)
	}
}

func TestPgHBASourceRanges(t *testing.T) {
	c := testCluster()
	if !slices.Contains(PgHBA(c), "host all all 10.0.0.0/8 scram-sha-256") {
		t.Errorf("default range missing: %v", PgHBA(c))
	}
	c.AllowedSourceRanges = []string{"203.0.113.7/32"}
	hba := PgHBA(c)
	if slices.Contains(hba, "host all all 10.0.0.0/8 scram-sha-256") || !slices.Contains(hba, "host all all 203.0.113.7/32 scram-sha-256") {
		t.Errorf("custom range not applied: %v", hba)
	}
}

func TestConfigureScriptKeepsEtcdDataAfterFirstRun(t *testing.T) {
	c := testCluster()
	script, err := ConfigureScript(c, c.Nodes[0])
	if err != nil {
		t.Fatal(err)
	}
	// The wipe must be guarded, or a rerun would destroy the etcd member.
	guard := strings.Index(script, `if [ ! -f "$mark" ]`)
	wipe := strings.Index(script, "rm -rf /var/lib/etcd/default")
	if guard < 0 || wipe < guard {
		t.Errorf("etcd data wipe is not guarded:\n%s", script)
	}
	if !strings.Contains(script, "systemctl start --no-block etcd") {
		t.Errorf("etcd must start without blocking until quorum")
	}
}

func TestParseEtcdHealth(t *testing.T) {
	c := testCluster()
	out := `http://10.0.0.1:2379 is healthy: successfully committed proposal: took = 2.1ms
http://10.0.0.3:2379 is healthy: successfully committed proposal: took = 3ms
http://10.0.0.2:2379 is unhealthy: failed to commit proposal: context deadline exceeded
Error: unhealthy cluster`
	h := ParseEtcdHealth(out, c)
	if !h["pg-0"] || h["pg-1"] || !h["pg-2"] {
		t.Errorf("health = %v", h)
	}
	if !HasQuorum(h) {
		t.Errorf("2 of 3 healthy should be a quorum")
	}
	h = ParseEtcdHealth("Error: context deadline exceeded", c)
	if h["pg-0"] || HasQuorum(h) {
		t.Errorf("no healthy line should mean no quorum: %v", h)
	}
}

func TestPlanMembershipReplacesLostMember(t *testing.T) {
	nodes := testCluster().Nodes
	nodes[0].PrivateIP = "10.0.0.9" // pg-0 was replaced
	out := []byte(`{"header":{"cluster_id":1},"members":[
		{"ID":4085538702289923226,"name":"pg-0","peerURLs":["http://10.0.0.1:2380"]},
		{"ID":2,"name":"pg-1","peerURLs":["http://10.0.0.2:2380"]},
		{"ID":3,"name":"pg-2","peerURLs":["http://10.0.0.3:2380"]},
		{"ID":4,"name":"","peerURLs":["http://10.0.0.7:2380"]}]}`)
	members, err := ParseEtcdMembers(out)
	if err != nil {
		t.Fatal(err)
	}
	remove, add := PlanMembership(members, nodes)
	if len(remove) != 2 || remove[0].HexID() != "38b2bfcdba19ec9a" || remove[1].ID != 4 {
		t.Errorf("remove = %+v, want the old pg-0 and the never-started member", remove)
	}
	if len(add) != 1 || add[0].Name != "pg-0" || PeerURL(add[0]) != "http://10.0.0.9:2380" {
		t.Errorf("add = %+v, want pg-0 at its new address", add)
	}

	// Nothing to do once the list matches, including a member added but not started yet.
	members[0] = EtcdMember{ID: 5, PeerURLs: []string{"http://10.0.0.9:2380"}}
	remove, add = PlanMembership(members[:3], nodes)
	if len(remove) != 0 || len(add) != 0 {
		t.Errorf("expected no changes, got remove=%+v add=%+v", remove, add)
	}
}

func TestJoiningMemberListsOnlyRegisteredMembers(t *testing.T) {
	c := testCluster()
	c.EtcdState = EtcdStateExisting
	c.EtcdMembers = c.Nodes[:2] // pg-2 is not added yet
	env := EtcdEnv(c, c.Nodes[0])
	if !strings.Contains(env, "ETCD_INITIAL_CLUSTER=pg-0=http://10.0.0.1:2380,pg-1=http://10.0.0.2:2380\n") {
		t.Errorf("initial cluster must match etcd's member list exactly:\n%s", env)
	}
}

func TestPlanMembershipAddsOneAtATime(t *testing.T) {
	nodes := testCluster().Nodes
	only := []EtcdMember{{ID: 2, Name: "pg-1", PeerURLs: []string{"http://10.0.0.2:2380"}}}
	_, add := PlanMembership(only, nodes)
	if len(add) != 1 || add[0].Name != "pg-0" {
		t.Errorf("add = %+v, want only pg-0", add)
	}
	// pg-0 added but not started: wait.
	pending := append(only, EtcdMember{ID: 9, PeerURLs: []string{"http://10.0.0.1:2380"}})
	if _, add = PlanMembership(pending, nodes); len(add) != 0 {
		t.Errorf("add = %+v, want nothing while pg-0 has not started", add)
	}
	if got := Registered(pending, nodes); len(got) != 2 || got[0].Name != "pg-0" || got[1].Name != "pg-1" {
		t.Errorf("registered = %+v", got)
	}
}

func TestParseMemberStatus(t *testing.T) {
	m, err := ParseMemberStatus("pg-1", []byte(`{"state":"running","role":"replica","timeline":3,"replication_state":"streaming"}`))
	if err != nil || m.Role != "replica" || m.State != "running" || m.Timeline != 3 {
		t.Errorf("got %+v, %v", m, err)
	}
	if _, err := ParseMemberStatus("pg-1", []byte("curl: (7) Failed to connect")); err == nil {
		t.Errorf("expected an error for no answer")
	}
}

func TestConfigureScriptJoinsExistingCluster(t *testing.T) {
	c := testCluster()
	c.EtcdState = EtcdStateExisting
	script, err := ConfigureScript(c, c.Nodes[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, "ETCD_INITIAL_CLUSTER_STATE=existing") {
		t.Errorf("etcd env does not join the existing cluster")
	}
	// A healthy local etcd must never be wiped, whatever the state.
	if !strings.Contains(script, `! ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379`) {
		t.Errorf("wipe is not guarded by the local health check:\n%s", script)
	}
}

func TestEtcdHealthNeedsAnAnswer(t *testing.T) {
	// A member still installing has no etcdctl: its output names no endpoint.
	ex := &scriptedExecutor{}
	if _, err := EtcdHealth(context.Background(), ex, &Node{Name: "pg-0"}, testCluster()); err == nil {
		t.Errorf("expected an error when etcdctl gives no health lines")
	}
}

// replyExecutor answers every command with reply.
type replyExecutor struct{ reply string }

func (e replyExecutor) Run(context.Context, *Node, string, []byte) (string, error) {
	return e.reply, nil
}

func TestEtcdClusterHealth(t *testing.T) {
	ex := replyExecutor{reply: `{"level":"warn","msg":"retrying of unary invoker failed"}
http://10.0.0.1:2379 is healthy: successfully committed proposal: took = 2ms
http://10.0.0.3:2379 is unhealthy: failed to commit proposal: context deadline exceeded
Error: unhealthy cluster`}
	h, err := EtcdClusterHealth(context.Background(), ex, &Node{Name: "pg-0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 2 || !h["http://10.0.0.1:2379"] || h["http://10.0.0.3:2379"] {
		t.Errorf("health = %v", h)
	}
	m := EtcdMember{ClientURLs: []string{"http://10.0.0.1:2379"}}
	if !MemberHealthy(m, h) {
		t.Errorf("pg-0 should be healthy")
	}
}

func TestSwitchoverRefused(t *testing.T) {
	ex := replyExecutor{reply: "candidate name does not match with sync_standby\n412"}
	err := Switchover(context.Background(), ex, &Node{Name: "pg-1", PrivateIP: "10.0.0.2"}, "pg-2", "pg-0")
	if err == nil || !strings.Contains(err.Error(), "HTTP 412") || !strings.Contains(err.Error(), "sync_standby") {
		t.Errorf("err = %v, want the refusal with its code and reason", err)
	}
	if err := Switchover(context.Background(), replyExecutor{reply: "Successfully switched over\n200"}, &Node{}, "a", "b"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseClusterStatus(t *testing.T) {
	body := `{"members":[
		{"name":"pg-0","role":"leader","state":"running","lag":0},
		{"name":"pg-1","role":"replica","state":"streaming","lag":128},
		{"name":"pg-2","role":"replica","state":"starting","lag":"unknown"}],"scope":"pg"}`
	members, err := ParseClusterStatus([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("got %d members", len(members))
	}
	if !IsLeader(members[0].Role) || !IsHealthy(members[1].State) || IsHealthy(members[2].State) {
		t.Errorf("unexpected roles/states: %+v", members)
	}
	if members[1].Lag == nil || *members[1].Lag != 128 {
		t.Errorf("lag of pg-1 = %v", members[1].Lag)
	}
	if members[2].Lag != nil {
		t.Errorf("unknown lag should be nil, got %d", *members[2].Lag)
	}
}

// scriptedExecutor answers the step check with a fixed state and records
// every command.
type scriptedExecutor struct {
	checkReply string
	commands   []string
	stdins     []string
}

func (e *scriptedExecutor) Run(_ context.Context, _ *Node, cmd string, stdin []byte) (string, error) {
	e.commands = append(e.commands, cmd)
	e.stdins = append(e.stdins, string(stdin))
	if strings.Contains(cmd, ".done 2>/dev/null") {
		return e.checkReply, nil
	}
	return "", nil
}

func TestRunStep(t *testing.T) {
	tests := []struct {
		reply     string
		want      StepState
		wantStart bool
	}{
		{reply: "done\n", want: StepDone},
		{reply: "running\n", want: StepRunning},
		{reply: "new\n", want: StepRunning, wantStart: true},
		{reply: "failed\nE: Unable to locate package\n", want: StepFailed, wantStart: true},
	}
	for _, tt := range tests {
		ex := &scriptedExecutor{checkReply: tt.reply}
		res, err := RunStep(context.Background(), ex, &Node{Name: "pg-0"}, "install", "echo hi")
		if err != nil {
			t.Fatal(err)
		}
		if res.State != tt.want {
			t.Errorf("reply %q: state = %s, want %s", tt.reply, res.State, tt.want)
		}
		started := len(ex.commands) == 2
		if started != tt.wantStart {
			t.Errorf("reply %q: started = %v, want %v", tt.reply, started, tt.wantStart)
		}
		if started && (!strings.Contains(ex.commands[1], "systemd-run --unit=operator-install") || ex.stdins[1] != "echo hi") {
			t.Errorf("reply %q: unexpected start command %q with stdin %q", tt.reply, ex.commands[1], ex.stdins[1])
		}
	}
	ex := &scriptedExecutor{checkReply: "failed\nE: Unable to locate package\n"}
	res, _ := RunStep(context.Background(), ex, &Node{Name: "pg-0"}, "install", "echo hi")
	if res.Log != "E: Unable to locate package" {
		t.Errorf("failed log = %q", res.Log)
	}
}

func TestGenerateKey(t *testing.T) {
	priv, pub, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKey(priv)
	if err != nil {
		t.Fatalf("private key does not parse: %v", err)
	}
	parsed, _, _, _, err := ssh.ParseAuthorizedKey([]byte(pub))
	if err != nil {
		t.Fatalf("public key does not parse: %v", err)
	}
	if ssh.FingerprintSHA256(parsed) != ssh.FingerprintSHA256(signer.PublicKey()) {
		t.Errorf("public key does not match private key")
	}
	if !strings.Contains(CloudInit(pub), pub) {
		t.Errorf("cloud-init does not include the key")
	}
}
