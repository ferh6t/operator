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

// Package postgres installs and configures etcd, Patroni and PostgreSQL on
// Droplets over SSH.
//
// Every member runs all three. Bootstrapping goes in this order:
//  1. install the packages (no peer information needed)
//  2. write the etcd and Patroni configs, which list every member, and start etcd
//  3. once etcd has a quorum, start Patroni: the first member to take the
//     leader lock runs initdb, the others clone it and stream from it
package postgres

import (
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// Ports used on every member.
const (
	EtcdClientPort = 2379
	EtcdPeerPort   = 2380
	PatroniAPIPort = 8008
	PostgresPort   = 5432
)

// DefaultAllowedSourceRange is the DigitalOcean private network.
const DefaultAllowedSourceRange = "10.0.0.0/8"

// Node is one member of the cluster.
type Node struct {
	// Name of the member, also its etcd and Patroni name.
	Name string
	// PublicIP is where the controller connects over SSH.
	PublicIP string
	// PrivateIP is what etcd, Patroni and replication use.
	PrivateIP string
	// HostKey is the expected SSH host key fingerprint. When empty, the first
	// connection records it here.
	HostKey string
}

// Cluster is everything needed to render the configs of every member.
type Cluster struct {
	// Name is the Patroni scope.
	Name string
	// Token makes the etcd cluster unique, so members of another cluster
	// cannot join it by mistake.
	Token string
	// Version is the PostgreSQL major version, e.g. "16".
	Version string
	// Nodes are all members, in index order.
	Nodes []Node

	SuperuserPassword   string
	ReplicationPassword string

	// AllowedSourceRanges may connect to PostgreSQL with a password.
	AllowedSourceRanges []string

	// EtcdState is EtcdStateNew until the cluster has run, then
	// EtcdStateExisting. Empty means new.
	EtcdState string

	// EtcdMembers are the nodes registered in etcd, when known. A member
	// joining a running cluster must list exactly these. Nil means every node.
	EtcdMembers []Node
}

// etcdState returns the state a member starts etcd with.
func (c Cluster) etcdState() string {
	if c.EtcdState == "" {
		return EtcdStateNew
	}
	return c.EtcdState
}

// CloudInit returns cloud-init user data that lets the controller log in as
// root with the given authorized_keys line.
func CloudInit(authorizedKey string) string {
	return fmt.Sprintf(`#cloud-config
write_files:
  - path: /root/.ssh/authorized_keys
    content: %q
    append: true
    permissions: "0600"
`, authorizedKey+"\n")
}

// InstallScript installs PostgreSQL, Patroni and etcd from the PostgreSQL
// apt repository, without starting them.
func InstallScript(version string) string {
	return fmt.Sprintf(`set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
APT="apt-get -y -q -o DPkg::Lock::Timeout=600"

echo "Waiting for cloud-init"
cloud-init status --wait >/dev/null 2>&1 || true

# Patroni creates the database itself, so the package must not create one.
mkdir -p /etc/postgresql-common
echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf

$APT update
$APT install postgresql-common curl ca-certificates
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
$APT install postgresql-%[1]s patroni python3-etcd etcd-server etcd-client

# The controller starts these once every member is configured.
systemctl disable --now etcd 2>/dev/null || true
systemctl disable --now patroni 2>/dev/null || true
echo "Installed PostgreSQL %[1]s, Patroni and etcd"
`, version)
}

// ConfigureScript writes the etcd and Patroni configs of one member and
// starts etcd. etcd blocks until a quorum of members is up, so it is started
// without waiting. Running it again rewrites the configs and reloads Patroni.
func ConfigureScript(c Cluster, n Node) (string, error) {
	patroni, err := PatroniConfig(c, n)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`set -euo pipefail
mkdir -p /var/lib/operator

# Start etcd from an empty data directory when this member has never been
# configured (the package started a throwaway single-member etcd), or when it
# was started for another state and is not a healthy member. A member whose
# local etcd is healthy always keeps its data.
mark=/var/lib/operator/etcd-initialized
if [ ! -f "$mark" ] || { [ "$(cat "$mark")" != "%[6]s" ] && \
     ! ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 --command-timeout=3s endpoint health >/dev/null 2>&1; }; then
  systemctl stop etcd 2>/dev/null || true
  rm -rf /var/lib/etcd/default
  echo "Reset etcd data for state %[6]s"
fi
echo "%[6]s" > "$mark"

cat > /etc/default/etcd <<'OPERATOR_EOF'
%[1]sOPERATOR_EOF

install -d -o postgres -g postgres -m 0750 /etc/patroni
cat > /etc/patroni/config.yml <<'OPERATOR_EOF'
%[2]sOPERATOR_EOF
chown postgres:postgres /etc/patroni/config.yml
chmod 0600 /etc/patroni/config.yml
install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/%[3]s/patroni

cat > /etc/systemd/system/patroni.service <<'OPERATOR_EOF'
%[4]sOPERATOR_EOF

systemctl daemon-reload
systemctl enable etcd
systemctl start --no-block etcd
if systemctl is-active --quiet patroni; then systemctl reload patroni; fi
echo "Configured member %[5]s"
`, EtcdEnv(c, n), patroni, c.Version, patroniUnit, n.Name, c.etcdState()), nil
}

// patroniUnit runs Patroni as the postgres user. It replaces the unit the
// package ships, which expects a different config path.
const patroniUnit = `[Unit]
Description=Patroni (managed by the operator)
After=network-online.target etcd.service
Wants=network-online.target

[Service]
Type=simple
User=postgres
Group=postgres
ExecStart=/usr/bin/patroni /etc/patroni/config.yml
ExecReload=/bin/kill -s HUP $MAINPID
KillMode=process
TimeoutSec=30
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
`

// EtcdEnv renders /etc/default/etcd for one member. The INITIAL_* settings
// are only read when the member starts with an empty data directory.
func EtcdEnv(c Cluster, n Node) string {
	initial := c.Nodes
	if c.EtcdMembers != nil {
		initial = c.EtcdMembers
	}
	peers := make([]string, 0, len(initial))
	for _, m := range initial {
		peers = append(peers, fmt.Sprintf("%s=http://%s:%d", m.Name, m.PrivateIP, EtcdPeerPort))
	}
	lines := []string{
		"ETCD_NAME=" + n.Name,
		"ETCD_DATA_DIR=/var/lib/etcd/default",
		fmt.Sprintf("ETCD_LISTEN_PEER_URLS=http://%s:%d", n.PrivateIP, EtcdPeerPort),
		fmt.Sprintf("ETCD_LISTEN_CLIENT_URLS=http://%s:%d,http://127.0.0.1:%d", n.PrivateIP, EtcdClientPort, EtcdClientPort),
		fmt.Sprintf("ETCD_INITIAL_ADVERTISE_PEER_URLS=http://%s:%d", n.PrivateIP, EtcdPeerPort),
		fmt.Sprintf("ETCD_ADVERTISE_CLIENT_URLS=http://%s:%d", n.PrivateIP, EtcdClientPort),
		"ETCD_INITIAL_CLUSTER=" + strings.Join(peers, ","),
		"ETCD_INITIAL_CLUSTER_STATE=" + c.etcdState(),
		"ETCD_INITIAL_CLUSTER_TOKEN=" + c.Token,
	}
	return strings.Join(lines, "\n") + "\n"
}

// EtcdEndpoints returns the client URLs of every member that has an address.
func EtcdEndpoints(c Cluster) string {
	urls := make([]string, 0, len(c.Nodes))
	for _, m := range c.Nodes {
		if m.PrivateIP == "" {
			continue
		}
		urls = append(urls, fmt.Sprintf("http://%s:%d", m.PrivateIP, EtcdClientPort))
	}
	return strings.Join(urls, ",")
}

// PatroniConfig renders /etc/patroni/config.yml for one member.
func PatroniConfig(c Cluster, n Node) (string, error) {
	etcdHosts := make([]string, 0, len(c.Nodes))
	for _, m := range c.Nodes {
		etcdHosts = append(etcdHosts, fmt.Sprintf("%s:%d", m.PrivateIP, EtcdClientPort))
	}

	cfg := map[string]any{
		"scope":     c.Name,
		"namespace": "/service/",
		"name":      n.Name,
		"restapi": map[string]any{
			"listen":          fmt.Sprintf("%s:%d", n.PrivateIP, PatroniAPIPort),
			"connect_address": fmt.Sprintf("%s:%d", n.PrivateIP, PatroniAPIPort),
		},
		"etcd3": map[string]any{
			"hosts": etcdHosts,
		},
		"bootstrap": map[string]any{
			"dcs": map[string]any{
				"ttl":                     30,
				"loop_wait":               10,
				"retry_timeout":           10,
				"maximum_lag_on_failover": 1048576,
				"postgresql": map[string]any{
					"use_pg_rewind": true,
					"parameters": map[string]any{
						"wal_level":             "replica",
						"hot_standby":           "on",
						"max_wal_senders":       10,
						"max_replication_slots": 10,
						"wal_log_hints":         "on",
					},
				},
			},
			"initdb": []any{
				map[string]any{"encoding": "UTF8"},
				"data-checksums",
			},
		},
		"postgresql": map[string]any{
			"listen":          fmt.Sprintf("0.0.0.0:%d", PostgresPort),
			"connect_address": fmt.Sprintf("%s:%d", n.PrivateIP, PostgresPort),
			"data_dir":        fmt.Sprintf("/var/lib/postgresql/%s/patroni", c.Version),
			"bin_dir":         fmt.Sprintf("/usr/lib/postgresql/%s/bin", c.Version),
			"pgpass":          "/var/lib/postgresql/.pgpass_patroni",
			"use_unix_socket": true,
			"authentication": map[string]any{
				"superuser": map[string]any{
					"username": "postgres",
					"password": c.SuperuserPassword,
				},
				"replication": map[string]any{
					"username": "replicator",
					"password": c.ReplicationPassword,
				},
			},
			"parameters": map[string]any{
				"unix_socket_directories": "/var/run/postgresql",
			},
			"pg_hba": PgHBA(c),
		},
		"tags": map[string]any{
			"nofailover":    false,
			"noloadbalance": false,
			"clonefrom":     false,
			"nosync":        false,
		},
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("rendering Patroni config: %w", err)
	}
	return string(out), nil
}

// PgHBA returns the pg_hba.conf lines Patroni writes on every member.
// Members always reach each other, so replication and pg_rewind keep working
// whatever allowedSourceRanges says.
func PgHBA(c Cluster) []string {
	hba := []string{
		"local all all peer",
		"host all all 127.0.0.1/32 scram-sha-256",
	}
	for _, m := range c.Nodes {
		hba = append(hba,
			fmt.Sprintf("host replication replicator %s/32 scram-sha-256", m.PrivateIP),
			fmt.Sprintf("host all all %s/32 scram-sha-256", m.PrivateIP),
		)
	}
	ranges := c.AllowedSourceRanges
	if len(ranges) == 0 {
		ranges = []string{DefaultAllowedSourceRange}
	}
	for _, r := range ranges {
		hba = append(hba, fmt.Sprintf("host all all %s scram-sha-256", r))
	}
	return hba
}
