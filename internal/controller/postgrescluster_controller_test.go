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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	webappv1 "github.com/ferh6t/operator/api/v1"
	"github.com/ferh6t/operator/internal/postgres"
)

// fakeExecutor stands in for SSH. Every step reports stepReply, etcd is
// healthy unless listed in etcdDown, and Patroni reports <name>-0 as leader
// unless clusterJSON is set.
type fakeExecutor struct {
	mu        sync.Mutex
	stepReply string
	// etcdDown lists private IPs whose etcd reports unhealthy.
	etcdDown map[string]bool
	// clusterJSON replaces Patroni's answer; %[1]s is the cluster name.
	clusterJSON string
	// etcd is etcd's member list. Nil means <name>-0..2 at 10.0.0.1..3.
	// member remove and member add change it like etcd does.
	etcd     []postgres.EtcdMember
	nextID   uint64
	commands []string
}

// clusterBody is Patroni's GET /cluster answer for the cluster of node.
func (f *fakeExecutor) clusterBody(node *postgres.Node) string {
	prefix := strings.TrimSuffix(node.Name, node.Name[strings.LastIndex(node.Name, "-"):])
	if f.clusterJSON != "" {
		return fmt.Sprintf(f.clusterJSON, prefix)
	}
	return fmt.Sprintf(`{"members":[
		{"name":"%[1]s-0","role":"leader","state":"running","timeline":2},
		{"name":"%[1]s-1","role":"replica","state":"streaming","lag":0},
		{"name":"%[1]s-2","role":"replica","state":"streaming","lag":0}]}`, prefix)
}

// etcdCommand answers etcdctl member commands.
func (f *fakeExecutor) etcdCommand(node *postgres.Node, cmd string) string {
	if f.etcd == nil {
		prefix := strings.TrimSuffix(node.Name, node.Name[strings.LastIndex(node.Name, "-"):])
		for i, id := range []uint64{4085538702289923226, 2, 3} {
			f.etcd = append(f.etcd, postgres.EtcdMember{ID: id, Name: fmt.Sprintf("%s-%d", prefix, i),
				PeerURLs: []string{fmt.Sprintf("http://10.0.0.%d:2380", i+1)}})
		}
	}
	fields := strings.Fields(cmd)
	switch {
	case strings.Contains(cmd, "member remove "):
		hex := fields[len(fields)-1]
		f.etcd = slices.DeleteFunc(f.etcd, func(m postgres.EtcdMember) bool { return m.HexID() == hex })
	case strings.Contains(cmd, "member add "):
		f.nextID++
		f.etcd = append(f.etcd, postgres.EtcdMember{ID: 100 + f.nextID,
			PeerURLs: []string{strings.TrimPrefix(fields[len(fields)-1], "--peer-urls=")}})
	}
	out, _ := json.Marshal(map[string]any{"members": f.etcd})
	return string(out)
}

// startMember marks an added etcd member as started, as etcd does when it joins.
func (f *fakeExecutor) startMember(name, ip string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.etcd {
		if slices.Contains(f.etcd[i].PeerURLs, "http://"+ip+":2380") {
			f.etcd[i].Name = name
		}
	}
	delete(f.etcdDown, ip)
}

func (f *fakeExecutor) Run(_ context.Context, node *postgres.Node, cmd string, _ []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, node.Name+": "+cmd)
	if node.HostKey == "" {
		node.HostKey = "SHA256:fake-" + node.Name
	}
	switch {
	case strings.Contains(cmd, ".done 2>/dev/null"):
		return f.stepReply, nil
	case strings.Contains(cmd, "endpoint health"):
		// Answer like etcdctl: one line per endpoint in --endpoints.
		list := strings.Fields(strings.SplitN(cmd, "--endpoints=", 2)[1])[0]
		var out strings.Builder
		for _, ep := range strings.Split(list, ",") {
			ip := strings.TrimSuffix(strings.TrimPrefix(ep, "http://"), ":2379")
			if f.etcdDown[ip] {
				fmt.Fprintf(&out, "%s is unhealthy: failed to commit proposal: context deadline exceeded\n", ep)
			} else {
				fmt.Fprintf(&out, "%s is healthy: successfully committed proposal: took = 2ms\n", ep)
			}
		}
		return out.String(), nil
	case strings.Contains(cmd, "etcdctl --command-timeout=5s member"):
		return f.etcdCommand(node, cmd), nil
	case strings.Contains(cmd, "/cluster"):
		return f.clusterBody(node), nil
	case strings.Contains(cmd, "/patroni"):
		// A member's own view: its entry in the cluster answer, if any.
		var c struct {
			Members []map[string]any `json:"members"`
		}
		_ = json.Unmarshal([]byte(f.clusterBody(node)), &c)
		for _, m := range c.Members {
			if m["name"] == node.Name {
				role := m["role"]
				if role == "leader" {
					role = "primary"
				}
				out, _ := json.Marshal(map[string]any{"state": m["state"], "role": role, "timeline": m["timeline"]})
				return string(out), nil
			}
		}
		return "", nil
	}
	return "", nil
}

func (f *fakeExecutor) ran(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.commands {
		if strings.Contains(c, substr) {
			return true
		}
	}
	return false
}

var _ = Describe("PostgresCluster Controller", func() {
	const namespace = "default"

	newCluster := func(name string) *webappv1.PostgresCluster {
		return &webappv1.PostgresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: webappv1.PostgresClusterSpec{
				Droplet: webappv1.DropletSpec{
					Region:         "fra1",
					Size:           "s-1vcpu-2gb-intel",
					Image:          "ubuntu-24-04-x64",
					TokenSecretRef: webappv1.SecretKeyReference{Name: "digitalocean-token"},
				},
			},
		}
	}

	// makeDropletsReady marks the cluster's Droplets Available with addresses,
	// as the Droplet controller would once their servers run.
	makeDropletsReady := func(pg *webappv1.PostgresCluster) {
		for i := range 3 {
			d := &webappv1.Droplet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%d", pg.Name, i), Namespace: namespace}, d)).To(Succeed())
			d.Status.PublicIPv4 = fmt.Sprintf("203.0.113.%d", i+1)
			d.Status.PrivateIPv4 = fmt.Sprintf("10.0.0.%d", i+1)
			meta.SetStatusCondition(&d.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionTrue, Reason: "Active"})
			Expect(k8sClient.Status().Update(ctx, d)).To(Succeed())
		}
	}

	cleanup := func(name string) {
		objs := []client.Object{
			&webappv1.PostgresCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-ssh", Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-credentials", Namespace: namespace}},
		}
		for i := range 3 {
			objs = append(objs, &webappv1.Droplet{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", name, i), Namespace: namespace}})
		}
		for _, o := range objs {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, o))).To(Succeed())
		}
	}

	reconcileOnce := func(r *PostgresClusterReconciler, name string) *webappv1.PostgresCluster {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		pg := &webappv1.PostgresCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pg)).To(Succeed())
		return pg
	}

	It("creates Secrets and its Droplets, then waits for them", func() {
		const name = "pg-wait"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())

		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		pg := reconcileOnce(r, name)

		By("defaulting replicas and version")
		Expect(pg.Spec.Replicas).To(Equal(int32(3)))
		Expect(pg.Spec.PostgresVersion).To(Equal("16"))

		By("generating an SSH key and passwords")
		sshSecret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-ssh", Namespace: namespace}, sshSecret)).To(Succeed())
		Expect(sshSecret.Type).To(Equal(corev1.SecretTypeSSHAuth))
		creds := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-credentials", Namespace: namespace}, creds)).To(Succeed())
		Expect(creds.Data[credPasswordKey]).NotTo(BeEmpty())
		Expect(creds.Data[credReplicationKey]).NotTo(BeEmpty())

		By("creating 3 owned Droplets whose cloud-init installs the controller's key")
		for i := range 3 {
			d := &webappv1.Droplet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%d", name, i), Namespace: namespace}, d)).To(Succeed())
			Expect(metav1.IsControlledBy(d, pg)).To(BeTrue())
			Expect(d.Labels).To(HaveKeyWithValue(webappv1.PostgresClusterLabel, name))
			Expect(d.Spec.Region).To(Equal("fra1"))
			Expect(d.Spec.UserData).To(ContainSubstring(strings.TrimSpace(string(sshSecret.Data[sshPublicKeyKey]))))
		}

		By("waiting without connecting while no Droplet is ready")
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseProvisioning))
		Expect(ex.commands).To(BeEmpty())

		By("keeping the same passwords on the next reconcile")
		reconcileOnce(r, name)
		again := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-credentials", Namespace: namespace}, again)).To(Succeed())
		Expect(again.Data[credPasswordKey]).To(Equal(creds.Data[credPasswordKey]))
	})

	It("reports Installing while the install step runs", func() {
		const name = "pg-install"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "running\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))

		pg := reconcileOnce(r, name)
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseInstalling))
		Expect(ex.ran("etcdctl")).To(BeFalse(), "etcd must not be checked before install finishes")
		Expect(ex.ran("patroni")).To(BeFalse())
	})

	It("bootstraps etcd and Patroni once every Droplet is ready", func() {
		const name = "pg-run"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))

		pg := reconcileOnce(r, name)
		Expect(ex.ran("endpoint health")).To(BeTrue())
		for i := range 3 {
			Expect(ex.ran(fmt.Sprintf("%s-%d: systemctl enable --now patroni", name, i))).To(BeTrue())
		}

		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseRunning))
		Expect(pg.Status.Primary).To(Equal(name + "-0"))
		Expect(pg.Status.ReadyMembers).To(Equal(int32(3)))
		Expect(meta.IsStatusConditionTrue(pg.Status.Conditions, "Available")).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(pg.Status.Conditions, "Initialized")).To(BeTrue())
		Expect(pg.Status.Members).To(HaveLen(3))
		Expect(pg.Status.Members[1].Role).To(Equal("replica"))
		Expect(pg.Status.Members[1].PrivateIPv4).To(Equal("10.0.0.2"))
		Expect(pg.Status.Members[0].HostKey).To(Equal("SHA256:fake-" + name + "-0"))
		Expect(pg.Status.Members[0].Timeline).To(Equal(int64(2)))
		Expect(pg.Status.Members[2].EtcdHealthy).To(HaveValue(BeTrue()))
		Expect(pg.Status.LastHealthCheck).NotTo(BeNil())
	})

	It("waits for every etcd member before the first Patroni start", func() {
		const name = "pg-etcdwait"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n", etcdDown: map[string]bool{"10.0.0.3": true}}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))

		pg := reconcileOnce(r, name)
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseBootstrapping))
		Expect(ex.ran("enable --now patroni")).To(BeFalse())
		Expect(pg.Status.Members[2].EtcdHealthy).To(HaveValue(BeFalse()))
		Expect(meta.FindStatusCondition(pg.Status.Conditions, "Available").Message).To(Equal("2 of 3 etcd members healthy"))
	})

	It("reports Degraded with Patroni's view when one etcd member goes down after running", func() {
		const name = "pg-degraded"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))
		Expect(reconcileOnce(r, name).Status.Phase).To(Equal(webappv1.PostgresPhaseRunning))

		ex.etcdDown = map[string]bool{"10.0.0.2": true}
		pg := reconcileOnce(r, name)
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseDegraded))
		Expect(pg.Status.Primary).To(Equal(name+"-0"), "Patroni is still asked while etcd has a quorum")
		Expect(pg.Status.Members[1].EtcdHealthy).To(HaveValue(BeFalse()))
		Expect(meta.IsStatusConditionTrue(pg.Status.Conditions, "Degraded")).To(BeTrue())
	})

	// replaceDroplet gives a Droplet new addresses, as when its server was
	// destroyed and the Droplet controller created a new one.
	replaceDroplet := func(name, public, private string) {
		d := &webappv1.Droplet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, d)).To(Succeed())
		d.Status.PublicIPv4, d.Status.PrivateIPv4 = public, private
		Expect(k8sClient.Status().Update(ctx, d)).To(Succeed())
	}
	// failedOver is Patroni after <name>-0 was lost: <name>-2 took over.
	const failedOver = `{"members":[
		{"name":"%[1]s-1","role":"replica","state":"streaming","lag":0,"timeline":3},
		{"name":"%[1]s-2","role":"leader","state":"running","timeline":3}]}`

	It("shows the current primary while a replaced Droplet is still installing", func() {
		const name = "pg-replaced"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))
		Expect(reconcileOnce(r, name).Status.Primary).To(Equal(name + "-0"))

		By("losing pg-0: Patroni fails over and a new, empty server takes its place")
		replaceDroplet(name+"-0", "203.0.113.9", "10.0.0.9")
		ex.etcdDown = map[string]bool{"10.0.0.9": true}
		ex.clusterJSON = failedOver
		ex.stepReply = "running\n"

		pg := reconcileOnce(r, name)
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseDegraded))
		Expect(meta.FindStatusCondition(pg.Status.Conditions, "Available").Reason).To(Equal("Installing"))
		Expect(pg.Status.Primary).To(Equal(name+"-2"), "status must not keep the old primary")
		Expect(pg.Status.ReadyMembers).To(Equal(int32(2)))
		Expect(pg.Status.Members[0].State).To(Equal("unknown"))
		Expect(pg.Status.Members[0].Role).To(BeEmpty())
		Expect(pg.Status.Members[0].EtcdHealthy).To(HaveValue(BeFalse()))
		Expect(pg.Status.Members[0].PublicIPv4).To(Equal("203.0.113.9"))
		Expect(pg.Status.Members[0].HostKey).To(Equal("SHA256:fake-"+name+"-0"), "the new server's key is pinned")
	})

	It("swaps a replaced Droplet into etcd before configuring it", func() {
		const name = "pg-swap"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))
		Expect(reconcileOnce(r, name).Status.Phase).To(Equal(webappv1.PostgresPhaseRunning))

		replaceDroplet(name+"-0", "203.0.113.9", "10.0.0.9")
		ex.etcdDown = map[string]bool{"10.0.0.9": true}
		ex.clusterJSON = failedOver
		// etcd still lists pg-0 at its old address; its ID 4085538702289923226 is 38b2bfcdba19ec9a in hex.

		pg := reconcileOnce(r, name)
		Expect(ex.ran("member remove 38b2bfcdba19ec9a")).To(BeTrue())
		Expect(ex.ran(fmt.Sprintf("member add %s-0 --peer-urls=http://10.0.0.9:2380", name))).To(BeTrue())
		Expect(ex.ran(name+"-0: ETCDCTL_API=3 etcdctl --command-timeout=5s member")).To(BeFalse(),
			"membership is changed through a healthy member, not the new one")
		Expect(pg.Status.Primary).To(Equal(name + "-2"))
		Expect(pg.Status.Phase).To(Equal(webappv1.PostgresPhaseDegraded))
	})

	It("adds two replaced Droplets back to etcd one at a time", func() {
		const name = "pg-two"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		ex := &fakeExecutor{stepReply: "done\n"}
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return ex }}
		reconcileOnce(r, name)
		makeDropletsReady(newCluster(name))
		Expect(reconcileOnce(r, name).Status.Phase).To(Equal(webappv1.PostgresPhaseRunning))

		By("losing pg-0 and pg-2, then recovering etcd on pg-1 alone (--force-new-cluster)")
		replaceDroplet(name+"-0", "203.0.113.9", "10.0.0.9")
		replaceDroplet(name+"-2", "203.0.113.8", "10.0.0.8")
		ex.etcdDown = map[string]bool{"10.0.0.9": true, "10.0.0.8": true}
		ex.etcd = []postgres.EtcdMember{{ID: 2, Name: name + "-1", PeerURLs: []string{"http://10.0.0.2:2380"}}}
		ex.clusterJSON = `{"members":[{"name":"%[1]s-1","role":"leader","state":"running","timeline":3}]}`

		pg := reconcileOnce(r, name)
		Expect(ex.ran(fmt.Sprintf("member add %s-0 ", name))).To(BeTrue())
		Expect(ex.ran(fmt.Sprintf("member add %s-2 ", name))).To(BeFalse(), "only one member is added at a time")
		Expect(pg.Status.Primary).To(Equal(name + "-1"))

		By("not adding the next one while the first has not started")
		reconcileOnce(r, name)
		Expect(ex.ran(fmt.Sprintf("member add %s-2 ", name))).To(BeFalse())

		By("adding the next one once the first has joined")
		ex.startMember(name+"-0", "10.0.0.9")
		reconcileOnce(r, name)
		Expect(ex.ran(fmt.Sprintf("member add %s-2 --peer-urls=http://10.0.0.8:2380", name))).To(BeTrue())
	})

	It("creates a Droplet again after it was deleted", func() {
		const name = "pg-recreate"
		DeferCleanup(cleanup, name)
		Expect(k8sClient.Create(ctx, newCluster(name))).To(Succeed())
		r := &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
			NewExecutor: func(ssh.Signer) postgres.Executor { return &fakeExecutor{stepReply: "done\n"} }}
		reconcileOnce(r, name)

		key := types.NamespacedName{Name: name + "-1", Namespace: namespace}
		first := &webappv1.Droplet{}
		Expect(k8sClient.Get(ctx, key, first)).To(Succeed())
		Expect(k8sClient.Delete(ctx, first)).To(Succeed())

		reconcileOnce(r, name)
		again := &webappv1.Droplet{}
		Expect(k8sClient.Get(ctx, key, again)).To(Succeed())
		Expect(again.UID).NotTo(Equal(first.UID))
	})

	It("rejects an even number of replicas", func() {
		pg := newCluster("pg-even")
		pg.Spec.Replicas = 2
		Expect(k8sClient.Create(ctx, pg)).NotTo(Succeed())
	})

	It("rejects a source range that is not a CIDR", func() {
		pg := newCluster("pg-cidr")
		pg.Spec.AllowedSourceRanges = []string{"0.0.0.0/0 trust"}
		Expect(k8sClient.Create(ctx, pg)).NotTo(Succeed())
	})
})
