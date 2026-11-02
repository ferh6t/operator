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
	"encoding/json"
	"fmt"
	"slices"
)

// Values for Cluster.EtcdState.
const (
	// EtcdStateNew bootstraps a new etcd cluster from every member at once.
	EtcdStateNew = "new"
	// EtcdStateExisting joins a member to a running etcd cluster. It is used
	// once the cluster has run, so a replaced Droplet joins instead of
	// trying to start a second cluster.
	EtcdStateExisting = "existing"
)

// EtcdMember is one entry of `etcdctl member list`.
type EtcdMember struct {
	ID       uint64   `json:"ID"`
	Name     string   `json:"name"`
	PeerURLs []string `json:"peerURLs"`
}

// HexID is the ID as etcdctl expects it in `member remove`.
func (m EtcdMember) HexID() string {
	return fmt.Sprintf("%x", m.ID)
}

// PeerURL is the etcd peer URL of a node.
func PeerURL(n Node) string {
	return fmt.Sprintf("http://%s:%d", n.PrivateIP, EtcdPeerPort)
}

// ParseEtcdMembers parses `etcdctl member list -w json`.
func ParseEtcdMembers(out []byte) ([]EtcdMember, error) {
	var resp struct {
		Members []EtcdMember `json:"members"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, fmt.Errorf("parsing etcd member list: %w", err)
	}
	return resp.Members, nil
}

// PlanMembership compares etcd's member list with the current nodes. It
// returns the members to remove (registered under a node's name, or with no
// name, at an address no node has any more: a Droplet that was replaced) and
// at most one node to add (no member has its address yet). Only one is added
// at a time, and none while an added member has not started: each added
// member counts towards the quorum before it can vote.
func PlanMembership(members []EtcdMember, nodes []Node) (remove []EtcdMember, add []Node) {
	current := map[string]bool{}
	names := map[string]bool{}
	for _, n := range nodes {
		current[PeerURL(n)] = true
		names[n.Name] = true
	}
	registered := map[string]bool{}
	for _, m := range members {
		live := slices.ContainsFunc(m.PeerURLs, func(u string) bool { return current[u] })
		for _, u := range m.PeerURLs {
			registered[u] = true
		}
		// A member added but never started has no name yet.
		if !live && (names[m.Name] || m.Name == "") {
			remove = append(remove, m)
		}
	}
	for _, m := range members {
		live := slices.ContainsFunc(m.PeerURLs, func(u string) bool { return current[u] })
		if live && m.Name == "" {
			// Added but not started yet: wait for it before adding another.
			return remove, nil
		}
	}
	for _, n := range nodes {
		if !registered[PeerURL(n)] {
			return remove, []Node{n}
		}
	}
	return remove, nil
}

// Registered returns the nodes etcd has a member for, in node order. Only
// these may start etcd once the cluster runs: a joining member must list
// exactly the registered members, itself included.
func Registered(members []EtcdMember, nodes []Node) []Node {
	urls := map[string]bool{}
	for _, m := range members {
		for _, u := range m.PeerURLs {
			urls[u] = true
		}
	}
	var out []Node
	for _, n := range nodes {
		if urls[PeerURL(n)] {
			out = append(out, n)
		}
	}
	return out
}

// listEtcdMembers runs `etcdctl member list` on via.
func listEtcdMembers(ctx context.Context, ex Executor, via *Node) ([]EtcdMember, error) {
	out, err := ex.Run(ctx, via, "ETCDCTL_API=3 etcdctl --command-timeout=5s member list -w json", nil)
	if err != nil {
		return nil, err
	}
	return ParseEtcdMembers([]byte(out))
}

// ReconcileEtcdMembers makes etcd's member list match the nodes, running
// etcdctl on via, which must be a healthy member. Stale members are removed
// before a new one is added, so the cluster never needs a larger quorum than
// it has. It returns the member list afterwards and what it changed.
func ReconcileEtcdMembers(ctx context.Context, ex Executor, via *Node, nodes []Node) ([]EtcdMember, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	members, err := listEtcdMembers(ctx, ex, via)
	if err != nil {
		return nil, nil, err
	}
	remove, add := PlanMembership(members, nodes)
	if len(remove) == 0 && len(add) == 0 {
		return members, nil, nil
	}

	var changes []string
	for _, m := range remove {
		cmd := "ETCDCTL_API=3 etcdctl --command-timeout=5s member remove " + m.HexID()
		if _, err := ex.Run(ctx, via, cmd, nil); err != nil {
			return nil, changes, fmt.Errorf("removing etcd member %s (%s): %w", m.Name, m.HexID(), err)
		}
		changes = append(changes, fmt.Sprintf("removed %s %v", m.Name, m.PeerURLs))
	}
	for _, n := range add {
		cmd := fmt.Sprintf("ETCDCTL_API=3 etcdctl --command-timeout=5s member add %s --peer-urls=%s", n.Name, PeerURL(n))
		if _, err := ex.Run(ctx, via, cmd, nil); err != nil {
			return nil, changes, fmt.Errorf("adding etcd member %s: %w", n.Name, err)
		}
		changes = append(changes, fmt.Sprintf("added %s %s", n.Name, PeerURL(n)))
	}
	members, err = listEtcdMembers(ctx, ex, via)
	return members, changes, err
}
