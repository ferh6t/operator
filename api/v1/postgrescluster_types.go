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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PostgresClusterLabel is put on every Droplet a PostgresCluster creates.
// Its value is the PostgresCluster name.
const PostgresClusterLabel = "webapp.ferh6t.github.io/postgrescluster"

// Phases reported in PostgresCluster status.phase.
const (
	PostgresPhaseProvisioning  = "Provisioning"
	PostgresPhaseInstalling    = "Installing"
	PostgresPhaseBootstrapping = "Bootstrapping"
	PostgresPhaseRunning       = "Running"
	PostgresPhaseScaling       = "Scaling"
	PostgresPhaseDegraded      = "Degraded"
)

// PostgresClusterSpec defines a highly available PostgreSQL cluster on
// DigitalOcean Droplets. Every Droplet runs etcd, Patroni and PostgreSQL:
// the etcd members form one cluster, and Patroni uses it to elect the primary.
type PostgresClusterSpec struct {
	// replicas is the number of Droplets (cluster members). It must be odd so
	// etcd keeps a quorum when one member is down. Change it only while the
	// cluster is Running: new members are added one at a time, and removed
	// members go highest number first, after the primary has moved away.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	// +kubebuilder:validation:XValidation:rule="self % 2 == 1",message="replicas must be odd so etcd keeps a quorum"
	Replicas int32 `json:"replicas,omitempty"`

	// postgresVersion is the PostgreSQL major version to install.
	// +optional
	// +kubebuilder:default="16"
	// +kubebuilder:validation:Enum="14";"15";"16";"17"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="postgresVersion is immutable"
	PostgresVersion string `json:"postgresVersion,omitempty"`

	// droplet describes every Droplet of the cluster. Each one is named
	// <cluster-name>-<index>. spec.name and spec.userData are ignored: the
	// controller sets cloud-init itself to install its SSH key.
	// The image must be Ubuntu 24.04 (ubuntu-24-04-x64).
	// +required
	Droplet DropletSpec `json:"droplet"`

	// allowedSourceRanges are the CIDRs allowed to connect to PostgreSQL with a
	// password. Defaults to the DigitalOcean private network (10.0.0.0/8).
	// Add "0.0.0.0/0" to allow connections from anywhere.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=43
	// +kubebuilder:validation:items:XValidation:rule="isCIDR(self)",message="must be a CIDR, e.g. 10.0.0.0/8"
	AllowedSourceRanges []string `json:"allowedSourceRanges,omitempty"`
}

// PostgresMember is one Droplet of the cluster, as seen by the controller
// and reported by Patroni.
type PostgresMember struct {
	// name of the member, equal to its Droplet name.
	// +required
	Name string `json:"name"`

	// publicIPv4 is the address the controller connects to over SSH.
	// +optional
	PublicIPv4 string `json:"publicIPv4,omitempty"`

	// privateIPv4 is the address etcd, Patroni and replication use.
	// +optional
	PrivateIPv4 string `json:"privateIPv4,omitempty"`

	// role reported by Patroni: leader, replica, sync_standby.
	// +optional
	Role string `json:"role,omitempty"`

	// state reported by Patroni: running, streaming, starting, stopped.
	// +optional
	State string `json:"state,omitempty"`

	// lagBytes is the replication lag in bytes. Empty for the leader.
	// +optional
	LagBytes *int64 `json:"lagBytes,omitempty"`

	// timeline is the PostgreSQL timeline Patroni reports. It goes up by one
	// on every failover.
	// +optional
	Timeline int64 `json:"timeline,omitempty"`

	// etcdHealthy is whether this member's etcd answered the last health check.
	// Empty until etcd has been checked.
	// +optional
	EtcdHealthy *bool `json:"etcdHealthy,omitempty"`

	// hostKey is the SHA256 fingerprint of the Droplet's SSH host key, recorded
	// on first connect. Later connections must present the same key.
	// +optional
	HostKey string `json:"hostKey,omitempty"`
}

// PostgresClusterStatus defines the observed state of PostgresCluster.
type PostgresClusterStatus struct {
	// phase is a short summary: Provisioning, Installing, Bootstrapping,
	// Running, Scaling or Degraded.
	// +optional
	Phase string `json:"phase,omitempty"`

	// primary is the name of the member that currently accepts writes.
	// +optional
	Primary string `json:"primary,omitempty"`

	// readyMembers is the number of members PostgreSQL reports as running.
	// +optional
	ReadyMembers int32 `json:"readyMembers"`

	// runningReplicas is spec.replicas the last time every member was
	// running. While it differs from spec.replicas the cluster is scaling.
	// +optional
	RunningReplicas int32 `json:"runningReplicas,omitempty"`

	// members of the cluster.
	// +optional
	// +listType=map
	// +listMapKey=name
	Members []PostgresMember `json:"members,omitempty"`

	// lastHealthCheck is when the controller last checked etcd and Patroni.
	// +optional
	LastHealthCheck *metav1.Time `json:"lastHealthCheck,omitempty"`

	// credentialsSecret is the Secret holding the PostgreSQL passwords
	// (keys: username, password, replication-password).
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`

	// sshSecret is the Secret holding the SSH key the controller uses to
	// reach the Droplets (type kubernetes.io/ssh-auth).
	// +optional
	SSHSecret string `json:"sshSecret,omitempty"`

	// observedGeneration is the most recent generation the controller acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the PostgresCluster.
	// Types: "Available", "Progressing", "Degraded", and "Initialized"
	// (True once every member has been running).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyMembers
// +kubebuilder:resource:scope=Namespaced,shortName=pgc
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyMembers`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.primary`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.postgresVersion`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgresCluster runs a Patroni-managed PostgreSQL cluster, backed by etcd,
// on DigitalOcean Droplets.
type PostgresCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresCluster
	// +required
	Spec PostgresClusterSpec `json:"spec"`

	// status defines the observed state of PostgresCluster
	// +optional
	Status PostgresClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresClusterList contains a list of PostgresCluster
type PostgresClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresCluster{}, &PostgresClusterList{})
		return nil
	})
}
