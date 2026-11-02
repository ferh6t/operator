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

package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
	"github.com/ferh6t/operator/internal/postgres"
	"github.com/ferh6t/operator/internal/ui"
)

// How long to wait before checking a PostgresCluster again.
const (
	// pgWaitInterval is used while Droplets boot or packages install.
	pgWaitInterval = 20 * time.Second
	// pgStepInterval is used between quick bootstrap steps.
	pgStepInterval = 10 * time.Second
	// pgHealthInterval is used to refresh the status of a running cluster.
	pgHealthInterval = 30 * time.Second
)

// Secret keys.
const (
	credUsernameKey    = "username"
	credPasswordKey    = "password"
	credReplicationKey = "replication-password"
	sshPublicKeyKey    = "ssh-publickey"
)

// PostgresClusterReconciler runs etcd, Patroni and PostgreSQL on Droplets it
// creates and owns, named <cluster>-0, <cluster>-1, ...
type PostgresClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewExecutor builds what runs commands on the Droplets. Tests replace it
	// with a fake. Nil means SSH.
	NewExecutor func(signer ssh.Signer) postgres.Executor

	// Tracker records each reconcile for the dashboard. It may be nil.
	Tracker *ui.Tracker
}

// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=postgresclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=postgresclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=postgresclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile moves a PostgresCluster one step closer to a running cluster.
// Each step is checked on every call, so it picks up where it left off.
func (r *PostgresClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	result, err := r.reconcile(ctx, req)
	if r.Tracker != nil {
		e := ui.Event{Time: time.Now(), Kind: ui.KindPostgresCluster, Namespace: req.Namespace, Name: req.Name,
			Duration: time.Since(start), RequeueIn: result.RequeueAfter}
		if err != nil {
			e.Error = err.Error()
		}
		pg := &webappv1.PostgresCluster{}
		if r.Get(ctx, req.NamespacedName, pg) == nil {
			e.Generation = pg.Generation
			// A Degraded condition is a failure even when no error was returned.
			if c := meta.FindStatusCondition(pg.Status.Conditions, "Degraded"); e.Error == "" && c != nil && c.Status == metav1.ConditionTrue {
				e.Error = c.Message
			}
		}
		r.Tracker.Record(e)
	}
	return result, err
}

// reconcile does the work for one PostgresCluster.
func (r *PostgresClusterReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logf.FromContext(ctx)

	pg := &webappv1.PostgresCluster{}
	if err := r.Get(ctx, req.NamespacedName, pg); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// The Droplets and Secrets are owned by the cluster, so Kubernetes
	// deletes them, and the Droplet finalizers delete the servers.
	if !pg.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if err := validateSourceRanges(pg.Spec.AllowedSourceRanges); err != nil {
		return r.degraded(ctx, pg, nil, "InvalidSpec", err)
	}

	signer, authorizedKey, err := r.ensureSSHSecret(ctx, pg)
	if err != nil {
		l.Error(err, "Failed to reconcile SSH Secret")
		return r.degraded(ctx, pg, nil, "SSHSecretError", err)
	}
	creds, err := r.ensureCredentials(ctx, pg)
	if err != nil {
		l.Error(err, "Failed to reconcile credentials Secret")
		return r.degraded(ctx, pg, nil, "CredentialsError", err)
	}
	if err := r.ensureDroplets(ctx, pg, authorizedKey); err != nil {
		l.Error(err, "Failed to reconcile Droplets")
		return r.degraded(ctx, pg, nil, "DropletError", err)
	}

	nodes, waiting, err := r.readyNodes(ctx, pg)
	if err != nil {
		return ctrl.Result{}, err
	}
	cluster := postgres.Cluster{
		Name:                pg.Name,
		Token:               string(pg.UID),
		Version:             pg.Spec.PostgresVersion,
		SuperuserPassword:   creds[credPasswordKey],
		ReplicationPassword: creds[credReplicationKey],
		AllowedSourceRanges: pg.Spec.AllowedSourceRanges,
		EtcdState:           postgres.EtcdStateNew,
	}
	if wasRunning(pg) {
		// A member set up from now on is a replacement: it joins the running
		// etcd cluster instead of starting a new one.
		cluster.EtcdState = postgres.EtcdStateExisting
	}
	for _, n := range nodes {
		if n.PrivateIP != "" {
			cluster.Nodes = append(cluster.Nodes, *n)
		}
	}
	ex := r.executor(signer)

	// Once the cluster has run, read its health first, so status shows what
	// is true now, even while a replaced Droplet is still being set up.
	view := healthView{}
	if wasRunning(pg) {
		if view, err = checkHealth(ctx, ex, pg, nodes, cluster); err != nil {
			return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseDegraded, err)
		}
	}
	applyView(pg, nodes, view)

	// Keep the proxy on the primary just found, even while the rest of this
	// reconcile is busy with scaling or a replaced member.
	if err := r.ensurePrimaryProxy(ctx, pg); err != nil {
		l.Error(err, "Failed to reconcile primary proxy")
	}

	// Members above spec.replicas leave first, one per reconcile.
	if stop, res, err := r.scaleDown(ctx, pg, nodes, ex); stop {
		return res, err
	}

	if waiting != "" {
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseProvisioning, "WaitingForDroplets", waiting, pgWaitInterval)
	}
	return r.bootstrap(ctx, pg, nodes, cluster, ex, view)
}

// scaleDown removes the highest-numbered member above spec.replicas. It does
// one thing per call and returns stop=true until no such member is left:
//  1. if the member is the primary, Patroni moves the primary to the most
//     caught-up remaining replica;
//  2. the member is removed from etcd, if the rest keep a quorum;
//  3. its Droplet is deleted, and the Droplet finalizer deletes the server.
func (r *PostgresClusterReconciler) scaleDown(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, ex postgres.Executor) (bool, ctrl.Result, error) {
	l := logf.FromContext(ctx)
	extras, err := r.extraDroplets(ctx, pg)
	if err != nil || len(extras) == 0 {
		return err != nil, ctrl.Result{}, err
	}
	d := extras[0]
	wait := func(msg string) (bool, ctrl.Result, error) {
		res, err := r.progress(ctx, pg, nodes, webappv1.PostgresPhaseScaling, "ScalingDown", msg, pgStepInterval)
		return true, res, err
	}
	if !d.DeletionTimestamp.IsZero() {
		return wait(fmt.Sprintf("Waiting for Droplet %s to be deleted", d.Name))
	}
	if !wasRunning(pg) {
		// Nothing runs on it yet, so there is nothing to hand over.
		return r.deleteMember(ctx, pg, nodes, d, "")
	}

	var via *postgres.Node
	for _, n := range orderByHealth(pg, nodes) {
		if isEtcdHealthy(pg, n.Name) {
			via = n
			break
		}
	}
	if via == nil {
		return wait(fmt.Sprintf("No healthy remaining member to remove %s through", d.Name))
	}

	// 1. Never remove the primary: move it to a remaining member first.
	members, err := postgres.ClusterStatus(ctx, ex, via)
	if err != nil {
		return wait(fmt.Sprintf("Waiting for Patroni before removing %s: %v", d.Name, err))
	}
	leader := ""
	for _, m := range members {
		if postgres.IsLeader(m.Role) {
			leader = m.Name
		}
	}
	if leader == "" {
		return wait(fmt.Sprintf("Waiting for a primary before removing %s", d.Name))
	}
	if leader == d.Name {
		candidate := switchoverCandidate(members, nodes)
		if candidate == "" {
			return wait(fmt.Sprintf("%s is the primary and no remaining replica is caught up to take over", d.Name))
		}
		if err := postgres.Switchover(ctx, ex, via, leader, candidate); err != nil {
			return wait(err.Error())
		}
		l.Info("Moved the primary before removing member", "from", leader, "to", candidate)
		return wait(fmt.Sprintf("Moved the primary from %s to %s before removing %s", leader, candidate, d.Name))
	}

	// 2. Remove it from etcd while the rest keep a quorum.
	etcdMembers, err := postgres.ListEtcdMembers(ctx, ex, via)
	if err != nil {
		return wait(fmt.Sprintf("Could not list etcd members: %v", err))
	}
	health, err := postgres.EtcdClusterHealth(ctx, ex, via)
	if err != nil {
		return wait(fmt.Sprintf("Could not check etcd health: %v", err))
	}
	peer := ""
	if d.Status.PrivateIPv4 != "" {
		peer = postgres.PeerURL(postgres.Node{PrivateIP: d.Status.PrivateIPv4})
	}
	idx := slices.IndexFunc(etcdMembers, func(m postgres.EtcdMember) bool {
		return m.Name == d.Name || (peer != "" && slices.Contains(m.PeerURLs, peer))
	})
	if idx >= 0 {
		target := etcdMembers[idx]
		rest := slices.Delete(slices.Clone(etcdMembers), idx, idx+1)
		up := 0
		for _, m := range rest {
			if postgres.MemberHealthy(m, health) {
				up++
			}
		}
		if up <= len(rest)/2 {
			return wait(fmt.Sprintf("Removing %s now would leave etcd without a quorum (%d of %d remaining members healthy)", d.Name, up, len(rest)))
		}
		if err := postgres.RemoveEtcdMember(ctx, ex, via, target); err != nil {
			return wait(err.Error())
		}
		l.Info("Removed etcd member", "member", d.Name, "via", via.Name)
	}

	// 3. Delete the Droplet.
	return r.deleteMember(ctx, pg, nodes, d, " after removing it from etcd")
}

// deleteMember deletes a member's Droplet; its finalizer deletes the server.
func (r *PostgresClusterReconciler) deleteMember(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, d *webappv1.Droplet, how string) (bool, ctrl.Result, error) {
	if err := r.Delete(ctx, d); client.IgnoreNotFound(err) != nil {
		return true, ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Deleted Droplet to scale down", "name", d.Name)
	res, err := r.progress(ctx, pg, nodes, webappv1.PostgresPhaseScaling, "ScalingDown",
		fmt.Sprintf("Deleting Droplet %s%s", d.Name, how), pgStepInterval)
	return true, res, err
}

// extraDroplets returns the cluster's Droplets above spec.replicas, highest
// number first.
func (r *PostgresClusterReconciler) extraDroplets(ctx context.Context, pg *webappv1.PostgresCluster) ([]*webappv1.Droplet, error) {
	list := &webappv1.DropletList{}
	if err := r.List(ctx, list, client.InNamespace(pg.Namespace), client.MatchingLabels{webappv1.PostgresClusterLabel: pg.Name}); err != nil {
		return nil, err
	}
	var extras []*webappv1.Droplet
	for i := range list.Items {
		d := &list.Items[i]
		if metav1.IsControlledBy(d, pg) && memberIndex(pg.Name, d.Name) >= int(pg.Spec.Replicas) {
			extras = append(extras, d)
		}
	}
	slices.SortFunc(extras, func(a, b *webappv1.Droplet) int {
		return memberIndex(pg.Name, b.Name) - memberIndex(pg.Name, a.Name)
	})
	return extras, nil
}

// memberIndex returns the index in a member name like pg-2. A name that is
// not <cluster>-<index> gets a high index, so it is removed.
func memberIndex(cluster, name string) int {
	var i int
	if _, err := fmt.Sscanf(name, cluster+"-%d", &i); err != nil || name != fmt.Sprintf("%s-%d", cluster, i) {
		return 1 << 20
	}
	return i
}

// switchoverCandidate picks the remaining member that is running and has the
// least replication lag, or "" when none is.
func switchoverCandidate(members []postgres.PatroniMember, nodes []*postgres.Node) string {
	best, bestLag := "", int64(-1)
	for _, m := range members {
		if postgres.IsLeader(m.Role) || !postgres.IsHealthy(m.State) {
			continue
		}
		if !slices.ContainsFunc(nodes, func(n *postgres.Node) bool { return n.Name == m.Name }) {
			continue
		}
		lag := int64(1 << 62)
		if m.Lag != nil {
			lag = *m.Lag
		}
		if best == "" || lag < bestLag {
			best, bestLag = m.Name, lag
		}
	}
	return best
}

// scaling reports whether spec.replicas differs from what the cluster last
// ran with, so missing or extra members are expected rather than an outage.
func scaling(pg *webappv1.PostgresCluster) bool {
	return wasRunning(pg) && pg.Status.RunningReplicas != 0 && pg.Status.RunningReplicas != pg.Spec.Replicas
}

// bootstrap runs the steps that need SSH: install, fix etcd membership,
// configure, wait for etcd, start Patroni, and read the cluster state.
// Every step is checked on every call; finished ones cost one SSH command.
func (r *PostgresClusterReconciler) bootstrap(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, cluster postgres.Cluster, ex postgres.Executor, view healthView) (ctrl.Result, error) {
	l := logf.FromContext(ctx)

	// 1. Install the packages on every member.
	done, msg, err := runOnAll(ctx, ex, nodes, "install", func(*postgres.Node) (string, error) {
		return postgres.InstallScript(cluster.Version), nil
	})
	if err != nil {
		return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseInstalling, err)
	}
	if !done {
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseInstalling, "Installing", msg, pgWaitInterval)
	}

	// 2. A replaced Droplet has a new address. etcd must forget the old one
	// and expect the new one before the new member's etcd starts. Members
	// are added one at a time, and only registered ones are configured.
	etcdNodes := nodes
	if wasRunning(pg) {
		stop, res, registered, err := r.reconcileEtcdMembers(ctx, pg, nodes, cluster, ex)
		if stop {
			return res, err
		}
		cluster.EtcdMembers = registered
		etcdNodes = filterNodes(nodes, registered)
	}

	// 3. Write the configs and start etcd.
	done, msg, err = runOnAll(ctx, ex, etcdNodes, "configure", func(n *postgres.Node) (string, error) {
		return postgres.ConfigureScript(cluster, *n)
	})
	if err != nil {
		return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, err)
	}
	if !done {
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, "Configuring", msg, pgStepInterval)
	}

	// 4. Make sure etcd runs on every configured member. A member that tried
	// to join before it was added exits and has to be started again.
	for _, n := range etcdNodes {
		if err := postgres.StartEtcd(ctx, ex, n); err != nil {
			if !wasRunning(pg) || errors.Is(err, postgres.ErrHostKeyMismatch) {
				return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, err)
			}
			l.Info("Could not reach member", "member", n.Name, "reason", err.Error())
		}
	}

	// 5. Patroni needs etcd, and etcd needs a quorum. Before the first start
	// every member must be healthy; afterwards a quorum is enough.
	etcd, err := etcdHealth(ctx, ex, orderByHealth(pg, nodes), cluster)
	if err != nil {
		return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, err)
	}
	view.etcd, view.checked = etcd, true
	// Quorum counts the members etcd has, which differs from the Droplets
	// while replaced ones are added back one at a time.
	registered := map[string]bool{}
	for _, n := range etcdNodes {
		registered[n.Name] = etcd[n.Name]
	}
	up := healthyCount(registered)
	if !postgres.HasQuorum(registered) && wasRunning(pg) {
		l.Info("etcd has lost quorum", "healthy", up, "members", len(registered))
		applyView(pg, nodes, view)
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseDegraded, "EtcdQuorumLost", quorumLostMessage(up, len(registered)), pgWaitInterval)
	}
	if !postgres.HasQuorum(registered) || (!wasRunning(pg) && up < len(nodes)) {
		l.Info("Waiting for etcd", "healthy", up, "members", len(nodes))
		applyView(pg, nodes, view)
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, "WaitingForEtcd",
			fmt.Sprintf("%d of %d etcd members healthy", up, len(nodes)), pgStepInterval)
	}

	// 6. Start Patroni on every configured member. The first member to take
	// the leader lock creates the database; the others clone it from the
	// leader. Once the cluster has run, a member that cannot be reached shows
	// up in the Patroni status below instead of hiding it.
	for _, n := range etcdNodes {
		if err := postgres.StartPatroni(ctx, ex, n); err != nil {
			if !wasRunning(pg) || errors.Is(err, postgres.ErrHostKeyMismatch) {
				return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, err)
			}
			l.Info("Could not reach member", "member", n.Name, "reason", err.Error())
		}
	}

	// 7. Ask every member's Patroni what it is doing.
	if view.patroni, err = patroniView(ctx, ex, orderByHealth(pg, nodes), true); err != nil {
		return r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, err)
	}
	if len(view.patroni) == 0 && !wasRunning(pg) {
		l.Info("Patroni is not answering yet")
		return r.progress(ctx, pg, nodes, webappv1.PostgresPhaseBootstrapping, "WaitingForPatroni",
			"Waiting for Patroni to start", pgStepInterval)
	}
	// After the first run, a member that does not answer is reported as unknown.
	return r.reportCluster(ctx, pg, nodes, view)
}

// reconcileEtcdMembers swaps replaced Droplets into etcd's member list and
// returns the nodes etcd now has a member for. It returns stop=true, with
// what Reconcile should return, when it cannot go on.
func (r *PostgresClusterReconciler) reconcileEtcdMembers(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, cluster postgres.Cluster, ex postgres.Executor) (bool, ctrl.Result, []postgres.Node, error) {
	var via *postgres.Node
	for _, n := range orderByHealth(pg, nodes) {
		if isEtcdHealthy(pg, n.Name) {
			via = n
			break
		}
	}
	if via == nil {
		up := 0
		for _, n := range nodes {
			if isEtcdHealthy(pg, n.Name) {
				up++
			}
		}
		res, err := r.progress(ctx, pg, nodes, webappv1.PostgresPhaseDegraded, "EtcdQuorumLost",
			quorumLostMessage(up, len(nodes)), pgWaitInterval)
		return true, res, nil, err
	}
	members, changes, err := postgres.ReconcileEtcdMembers(ctx, ex, via, cluster.Nodes)
	if len(changes) > 0 {
		logf.FromContext(ctx).Info("Updated etcd members", "via", via.Name, "changes", strings.Join(changes, "; "))
	}
	if err != nil {
		res, err := r.sshFailed(ctx, pg, nodes, webappv1.PostgresPhaseDegraded, fmt.Errorf("updating etcd members: %w", err))
		return true, res, nil, err
	}
	return false, ctrl.Result{}, postgres.Registered(members, cluster.Nodes), nil
}

// filterNodes returns the nodes whose name is in keep, in their own order.
func filterNodes(nodes []*postgres.Node, keep []postgres.Node) []*postgres.Node {
	var out []*postgres.Node
	for _, n := range nodes {
		if slices.ContainsFunc(keep, func(k postgres.Node) bool { return k.Name == n.Name }) {
			out = append(out, n)
		}
	}
	return out
}

// quorumLostMessage explains a lost etcd quorum. The controller cannot fix
// it: etcd refuses every membership change without a quorum.
func quorumLostMessage(up, total int) string {
	return fmt.Sprintf("etcd has lost quorum (%d of %d members healthy), so Patroni cannot elect a primary and "+
		"members cannot be replaced. Restart a surviving member with --force-new-cluster to recover; "+
		"the controller then adds the others back one at a time", up, total)
}

// healthView is what etcd and Patroni reported during one reconcile.
type healthView struct {
	// etcd is each member's etcd health by name. Nil when not checked.
	etcd map[string]bool
	// patroni is Patroni's view by member name. Nil when not checked; a
	// member missing from a checked view is reported as unknown.
	patroni map[string]postgres.PatroniMember
	// checked is true when anything was actually asked.
	checked bool
}

// checkHealth asks etcd and Patroni about every member, from whichever
// member answers first. A member that cannot be asked counts as down. Only a
// changed SSH host key is returned as an error.
func checkHealth(ctx context.Context, ex postgres.Executor, pg *webappv1.PostgresCluster, nodes []*postgres.Node, c postgres.Cluster) (healthView, error) {
	v := healthView{etcd: map[string]bool{}, patroni: map[string]postgres.PatroniMember{}, checked: true}
	for _, n := range nodes {
		v.etcd[n.Name] = false
	}
	reachable := orderByHealth(pg, nodes)

	etcd, err := etcdHealth(ctx, ex, reachable, c)
	if errors.Is(err, postgres.ErrHostKeyMismatch) {
		return v, err
	}
	maps.Copy(v.etcd, etcd)

	if v.patroni, err = patroniView(ctx, ex, reachable, postgres.HasQuorum(v.etcd)); err != nil {
		return v, err
	}
	return v, nil
}

// patroniView asks every member's own Patroni what it is doing. A member that
// does not answer is left out, so it shows as unknown. With an etcd quorum,
// the replication lag comes from Patroni's cluster view; without one that
// view is stale and is not used.
func patroniView(ctx context.Context, ex postgres.Executor, nodes []*postgres.Node, quorum bool) (map[string]postgres.PatroniMember, error) {
	view := map[string]postgres.PatroniMember{}
	for _, n := range nodes {
		m, err := postgres.MemberStatus(ctx, ex, n)
		if errors.Is(err, postgres.ErrHostKeyMismatch) {
			return view, err
		}
		if err == nil {
			view[n.Name] = m
		}
	}
	if !quorum || len(view) == 0 {
		return view, nil
	}
	members, err := clusterStatus(ctx, ex, nodes)
	if errors.Is(err, postgres.ErrHostKeyMismatch) {
		return view, err
	}
	for _, c := range members {
		if m, ok := view[c.Name]; ok {
			m.Lag = c.Lag
			view[c.Name] = m
		}
	}
	return view, nil
}

// orderByHealth returns the members that have addresses, the ones whose etcd
// was healthy at the last check first, so health questions go to a member
// likely to answer rather than to a Droplet that was just replaced.
func orderByHealth(pg *webappv1.PostgresCluster, nodes []*postgres.Node) []*postgres.Node {
	out := make([]*postgres.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.PublicIP != "" && n.PrivateIP != "" {
			out = append(out, n)
		}
	}
	slices.SortStableFunc(out, func(a, b *postgres.Node) int {
		ha, hb := isEtcdHealthy(pg, a.Name), isEtcdHealthy(pg, b.Name)
		switch {
		case ha && !hb:
			return -1
		case hb && !ha:
			return 1
		}
		return 0
	})
	return out
}

// isEtcdHealthy reports whether status last saw this member's etcd healthy.
func isEtcdHealthy(pg *webappv1.PostgresCluster, name string) bool {
	idx := slices.IndexFunc(pg.Status.Members, func(m webappv1.PostgresMember) bool { return m.Name == name })
	return idx >= 0 && pg.Status.Members[idx].EtcdHealthy != nil && *pg.Status.Members[idx].EtcdHealthy
}

// etcdHealth asks each member in turn until one answers, and returns the
// health of every etcd member by name.
func etcdHealth(ctx context.Context, ex postgres.Executor, nodes []*postgres.Node, c postgres.Cluster) (map[string]bool, error) {
	var errs []error
	for _, n := range nodes {
		health, err := postgres.EtcdHealth(ctx, ex, n, c)
		if err == nil {
			return health, nil
		}
		if errors.Is(err, postgres.ErrHostKeyMismatch) {
			return nil, err
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil, errors.New("no member has an address yet")
	}
	return nil, errors.Join(errs...)
}

// healthyCount returns how many members are healthy.
func healthyCount(health map[string]bool) int {
	n := 0
	for _, ok := range health {
		if ok {
			n++
		}
	}
	return n
}

// runOnAll runs one step on every member. It returns true when the step is
// done everywhere, or a message saying what is still going on.
func runOnAll(ctx context.Context, ex postgres.Executor, nodes []*postgres.Node, step string, script func(*postgres.Node) (string, error)) (bool, string, error) {
	var running, failed []string
	for _, n := range nodes {
		s, err := script(n)
		if err != nil {
			return false, "", err
		}
		res, err := postgres.RunStep(ctx, ex, n, step, s)
		if err != nil {
			return false, "", err
		}
		switch res.State {
		case postgres.StepRunning:
			running = append(running, n.Name)
		case postgres.StepFailed:
			failed = append(failed, fmt.Sprintf("%s (retrying): %s", n.Name, lastLines(res.Log, 3)))
		}
	}
	if len(failed) > 0 {
		return false, fmt.Sprintf("Step %s failed on %s", step, strings.Join(failed, "; ")), nil
	}
	if len(running) > 0 {
		return false, fmt.Sprintf("Step %s running on %s", step, strings.Join(running, ", ")), nil
	}
	return true, "", nil
}

// clusterStatus asks each member in turn until one answers.
func clusterStatus(ctx context.Context, ex postgres.Executor, nodes []*postgres.Node) ([]postgres.PatroniMember, error) {
	var errs []error
	for _, n := range nodes {
		members, err := postgres.ClusterStatus(ctx, ex, n)
		if err == nil {
			return members, nil
		}
		if errors.Is(err, postgres.ErrHostKeyMismatch) {
			return nil, err
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil, errors.New("no member has an address yet")
	}
	return nil, errors.Join(errs...)
}

// applyView writes a health view into status: members, primary and ready
// count. Nothing is carried over from earlier checks.
func applyView(pg *webappv1.PostgresCluster, nodes []*postgres.Node, v healthView) {
	pg.Status.Members = buildMembers(nodes, v)
	primary, ready := "", 0
	for _, m := range pg.Status.Members {
		if postgres.IsLeader(m.Role) {
			primary = m.Name
		}
		if postgres.IsHealthy(m.State) {
			ready++
		}
	}
	pg.Status.Primary = primary
	pg.Status.ReadyMembers = int32(ready)
	if v.checked {
		now := metav1.Now()
		pg.Status.LastHealthCheck = &now
	}
}

// reportCluster writes the health view to status and sets the phase.
func (r *PostgresClusterReconciler) reportCluster(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, v healthView) (ctrl.Result, error) {
	applyView(pg, nodes, v)
	primary, ready := pg.Status.Primary, int(pg.Status.ReadyMembers)

	etcdUp := healthyCount(v.etcd)
	msg := fmt.Sprintf("%d of %d members running, %d of %d etcd members healthy, primary %q", ready, len(nodes), etcdUp, len(nodes), primary)
	if primary != "" && ready == len(nodes) && etcdUp == len(nodes) {
		pg.Status.Phase = webappv1.PostgresPhaseRunning
		setPGCondition(pg, "Available", metav1.ConditionTrue, "Running", msg)
		setPGCondition(pg, "Progressing", metav1.ConditionFalse, "Running", msg)
		setPGCondition(pg, "Initialized", metav1.ConditionTrue, "Bootstrapped", "Every member has been running at least once")
		meta.RemoveStatusCondition(&pg.Status.Conditions, "Degraded")
		pg.Status.RunningReplicas = pg.Spec.Replicas
	} else if scaling(pg) && primary != "" {
		// New members are still joining; the database keeps serving.
		pg.Status.Phase = webappv1.PostgresPhaseScaling
		setPGCondition(pg, "Available", metav1.ConditionTrue, "Scaling", msg)
		setPGCondition(pg, "Progressing", metav1.ConditionTrue, "Scaling", msg)
		meta.RemoveStatusCondition(&pg.Status.Conditions, "Degraded")
	} else if wasRunning(pg) {
		pg.Status.Phase = webappv1.PostgresPhaseDegraded
		setPGCondition(pg, "Available", metav1.ConditionFalse, "Degraded", msg)
		setPGCondition(pg, "Degraded", metav1.ConditionTrue, "MembersDown", msg)
	} else {
		pg.Status.Phase = webappv1.PostgresPhaseBootstrapping
		setPGCondition(pg, "Available", metav1.ConditionFalse, "Bootstrapping", msg)
		setPGCondition(pg, "Progressing", metav1.ConditionTrue, "Bootstrapping", msg)
	}
	if err := r.writeStatus(ctx, pg); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensurePrimaryProxy(ctx, pg); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to reconcile primary proxy")
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pgHealthInterval}, nil
}

// wasRunning reports whether the cluster has been fully up before, so a
// problem now is a degradation rather than part of bootstrapping.
func wasRunning(pg *webappv1.PostgresCluster) bool {
	return meta.IsStatusConditionTrue(pg.Status.Conditions, "Initialized")
}

// readyNodes returns the members once every Droplet is Available and has
// both addresses. Otherwise it returns what it is waiting for.
func (r *PostgresClusterReconciler) readyNodes(ctx context.Context, pg *webappv1.PostgresCluster) ([]*postgres.Node, string, error) {
	list := &webappv1.DropletList{}
	if err := r.List(ctx, list, client.InNamespace(pg.Namespace), client.MatchingLabels{webappv1.PostgresClusterLabel: pg.Name}); err != nil {
		return nil, "", err
	}

	want := int(pg.Spec.Replicas)
	byName := map[string]*webappv1.Droplet{}
	for i := range list.Items {
		// A Droplet being deleted is gone as far as the cluster is concerned;
		// ensureDroplets creates its replacement once it is.
		if d := &list.Items[i]; metav1.IsControlledBy(d, pg) && d.DeletionTimestamp.IsZero() {
			byName[d.Name] = d
		}
	}
	known := map[string]string{}
	for _, m := range pg.Status.Members {
		known[m.Name+"/"+m.PublicIPv4] = m.HostKey
	}

	nodes := make([]*postgres.Node, 0, want)
	ready := 0
	for i := range want {
		name := fmt.Sprintf("%s-%d", pg.Name, i)
		d, ok := byName[name]
		if !ok {
			continue
		}
		n := &postgres.Node{Name: name, PublicIP: d.Status.PublicIPv4, PrivateIP: d.Status.PrivateIPv4}
		// A host key belongs to one server: forget it when the address changes.
		n.HostKey = known[name+"/"+n.PublicIP]
		nodes = append(nodes, n)
		if meta.IsStatusConditionTrue(d.Status.Conditions, "Available") && n.PublicIP != "" && n.PrivateIP != "" {
			ready++
		}
	}
	if ready < want {
		return nodes, fmt.Sprintf("%d of %d Droplets ready", ready, want), nil
	}
	return nodes, "", nil
}

// ensureSSHSecret returns the key the controller logs in with, creating it
// on first use. The public key reaches the Droplets through cloud-init.
func (r *PostgresClusterReconciler) ensureSSHSecret(ctx context.Context, pg *webappv1.PostgresCluster) (ssh.Signer, string, error) {
	name := pg.Name + "-ssh"
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pg.Namespace, Name: name}, secret)
	if apierrors.IsNotFound(err) {
		priv, pub, gerr := postgres.GenerateKey()
		if gerr != nil {
			return nil, "", gerr
		}
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pg.Namespace, Labels: map[string]string{webappv1.PostgresClusterLabel: pg.Name}},
			Type:       corev1.SecretTypeSSHAuth,
			Data: map[string][]byte{
				corev1.SSHAuthPrivateKey: priv,
				sshPublicKeyKey:          []byte(pub),
			},
		}
		if err := controllerutil.SetControllerReference(pg, secret, r.Scheme); err != nil {
			return nil, "", err
		}
		if err := r.Create(ctx, secret); err != nil {
			return nil, "", err
		}
		logf.FromContext(ctx).Info("Created SSH Secret", "name", name)
	} else if err != nil {
		return nil, "", err
	}
	pg.Status.SSHSecret = name

	signer, err := ssh.ParsePrivateKey(secret.Data[corev1.SSHAuthPrivateKey])
	if err != nil {
		return nil, "", fmt.Errorf("parsing %s in Secret %q: %w", corev1.SSHAuthPrivateKey, name, err)
	}
	pub := strings.TrimSpace(string(secret.Data[sshPublicKeyKey]))
	if pub == "" {
		pub = strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))
	}
	return signer, pub, nil
}

// ensureCredentials returns the PostgreSQL passwords, generating them on
// first use. Existing passwords are never changed.
func (r *PostgresClusterReconciler) ensureCredentials(ctx context.Context, pg *webappv1.PostgresCluster) (map[string]string, error) {
	name := pg.Name + "-credentials"
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pg.Namespace, Name: name}, secret)
	if apierrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pg.Namespace, Labels: map[string]string{webappv1.PostgresClusterLabel: pg.Name}},
			Type:       corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				credUsernameKey:    []byte("postgres"),
				credPasswordKey:    []byte(postgres.GeneratePassword()),
				credReplicationKey: []byte(postgres.GeneratePassword()),
			},
		}
		if err := controllerutil.SetControllerReference(pg, secret, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, secret); err != nil {
			return nil, err
		}
		logf.FromContext(ctx).Info("Created credentials Secret", "name", name)
	} else if err != nil {
		return nil, err
	}
	pg.Status.CredentialsSecret = name

	creds := map[string]string{}
	for _, k := range []string{credPasswordKey, credReplicationKey} {
		v := string(secret.Data[k])
		if v == "" {
			return nil, fmt.Errorf("secret %q has no key %q", name, k)
		}
		creds[k] = v
	}
	return creds, nil
}

// ensureDroplets creates every missing member Droplet, <cluster>-0 to
// <cluster>-<replicas-1>. Existing Droplets are left alone: a Droplet's spec
// only matters when its server is created. A Droplet being deleted is
// recreated once it is gone, which the Owns watch reports.
func (r *PostgresClusterReconciler) ensureDroplets(ctx context.Context, pg *webappv1.PostgresCluster, authorizedKey string) error {
	for i := range int(pg.Spec.Replicas) {
		name := fmt.Sprintf("%s-%d", pg.Name, i)
		existing := &webappv1.Droplet{}
		err := r.Get(ctx, types.NamespacedName{Namespace: pg.Namespace, Name: name}, existing)
		if err == nil {
			if !metav1.IsControlledBy(existing, pg) {
				return fmt.Errorf("droplet %q already exists and is not owned by PostgresCluster %q", name, pg.Name)
			}
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}

		d := &webappv1.Droplet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: pg.Namespace,
				Labels:    map[string]string{webappv1.PostgresClusterLabel: pg.Name},
			},
			Spec: *pg.Spec.Droplet.DeepCopy(),
		}
		// Each server is named after its object, and cloud-init installs the
		// controller's SSH key.
		d.Spec.Name = ""
		d.Spec.UserData = postgres.CloudInit(authorizedKey)
		if err := controllerutil.SetControllerReference(pg, d, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, d); err != nil {
			return err
		}
		logf.FromContext(ctx).Info("Created Droplet", "name", name)
	}
	return nil
}

// executor returns what runs commands on the Droplets.
func (r *PostgresClusterReconciler) executor(signer ssh.Signer) postgres.Executor {
	if r.NewExecutor != nil {
		return r.NewExecutor(signer)
	}
	return &postgres.SSHExecutor{Signer: signer}
}

// progress records that the cluster is still being set up, or, once it has
// run, that it is degraded, and checks again later.
func (r *PostgresClusterReconciler) progress(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, phase, reason, msg string, after time.Duration) (ctrl.Result, error) {
	syncHostKeys(pg, nodes)
	if scaling(pg) && phase != webappv1.PostgresPhaseDegraded {
		// Adding or removing members: the database keeps serving.
		pg.Status.Phase = webappv1.PostgresPhaseScaling
		setPGCondition(pg, "Progressing", metav1.ConditionTrue, reason, msg)
		return ctrl.Result{RequeueAfter: after}, r.writeStatus(ctx, pg)
	}
	setPGCondition(pg, "Available", metav1.ConditionFalse, reason, msg)
	if wasRunning(pg) {
		// It was up before, so this is an outage, not setup.
		pg.Status.Phase = webappv1.PostgresPhaseDegraded
		setPGCondition(pg, "Degraded", metav1.ConditionTrue, reason, msg)
	} else {
		pg.Status.Phase = phase
		setPGCondition(pg, "Progressing", metav1.ConditionTrue, reason, msg)
		meta.RemoveStatusCondition(&pg.Status.Conditions, "Degraded")
	}
	return ctrl.Result{RequeueAfter: after}, r.writeStatus(ctx, pg)
}

// sshFailed handles an error reaching a member. A changed host key needs a
// person to look at it; anything else is usually a Droplet still booting.
func (r *PostgresClusterReconciler) sshFailed(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, phase string, err error) (ctrl.Result, error) {
	if errors.Is(err, postgres.ErrHostKeyMismatch) {
		logf.FromContext(ctx).Error(err, "Refused to connect to member")
		return r.degraded(ctx, pg, nodes, "HostKeyMismatch", err)
	}
	logf.FromContext(ctx).Info("Could not reach member", "reason", err.Error())
	return r.progress(ctx, pg, nodes, phase, "WaitingForSSH", err.Error(), pgWaitInterval)
}

// degraded records an error that needs attention and checks again later.
func (r *PostgresClusterReconciler) degraded(ctx context.Context, pg *webappv1.PostgresCluster, nodes []*postgres.Node, reason string, err error) (ctrl.Result, error) {
	syncHostKeys(pg, nodes)
	pg.Status.Phase = webappv1.PostgresPhaseDegraded
	setPGCondition(pg, "Degraded", metav1.ConditionTrue, reason, err.Error())
	setPGCondition(pg, "Available", metav1.ConditionFalse, reason, err.Error())
	return ctrl.Result{RequeueAfter: pgWaitInterval}, r.writeStatus(ctx, pg)
}

// buildMembers turns nodes, plus what this reconcile checked, into status
// members. A member Patroni does not list is reported as unknown.
func buildMembers(nodes []*postgres.Node, v healthView) []webappv1.PostgresMember {
	members := make([]webappv1.PostgresMember, 0, len(nodes))
	for _, n := range nodes {
		m := webappv1.PostgresMember{Name: n.Name, PublicIPv4: n.PublicIP, PrivateIPv4: n.PrivateIP, HostKey: n.HostKey}
		if healthy, ok := v.etcd[n.Name]; ok {
			m.EtcdHealthy = &healthy
		}
		if p, ok := v.patroni[n.Name]; ok {
			m.Role, m.State, m.LagBytes, m.Timeline = p.Role, p.State, p.Lag, p.Timeline
		} else if v.patroni != nil {
			m.State = "unknown"
		}
		members = append(members, m)
	}
	return members
}

// syncHostKeys copies host keys recorded during this reconcile into status,
// so a key seen for the first time is pinned even when the reconcile stops early.
func syncHostKeys(pg *webappv1.PostgresCluster, nodes []*postgres.Node) {
	for i := range pg.Status.Members {
		m := &pg.Status.Members[i]
		for _, n := range nodes {
			if n.Name == m.Name && n.PublicIP == m.PublicIPv4 && n.HostKey != "" {
				m.HostKey = n.HostKey
			}
		}
	}
}

// writeStatus saves status on top of the latest version of the object.
func (r *PostgresClusterReconciler) writeStatus(ctx context.Context, pg *webappv1.PostgresCluster) error {
	pg.Status.ObservedGeneration = pg.Generation
	// Another write may land between reading and updating; status is all
	// this reconcile's own view, so read again and retry.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &webappv1.PostgresCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pg), latest); err != nil {
			return err
		}
		latest.Status = pg.Status
		return r.Status().Update(ctx, latest)
	})
	if client.IgnoreNotFound(err) != nil {
		logf.FromContext(ctx).Error(err, "Failed to update PostgresCluster status")
		return err
	}
	return nil
}

// validateSourceRanges checks that every entry is a CIDR, since each one
// becomes a pg_hba.conf line.
func validateSourceRanges(ranges []string) error {
	for _, r := range ranges {
		if _, _, err := net.ParseCIDR(r); err != nil {
			return fmt.Errorf("allowedSourceRanges: %q is not a CIDR", r)
		}
	}
	return nil
}

// lastLines returns at most n trailing lines of s, joined by " | ".
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// setPGCondition adds or updates one status condition.
func setPGCondition(pg *webappv1.PostgresCluster, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&pg.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pg.Generation,
	})
}

// SetupWithManager sets up the controller with the Manager. Owning the
// Droplets means a Droplet becoming ready, or being deleted, reconciles the
// cluster right away.
func (r *PostgresClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.PostgresCluster{}).
		Owns(&webappv1.Droplet{}).
		Owns(&corev1.Secret{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("postgrescluster").
		Complete(r)
}
