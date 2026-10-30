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

// SecretKeyReference points to a key inside a Secret in the same namespace.
type SecretKeyReference struct {
	// name of the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// key inside the Secret that holds the value.
	// +optional
	// +kubebuilder:default="token"
	Key string `json:"key,omitempty"`
}

// DropletSpec defines the desired state of a DigitalOcean Droplet.
// Field names follow the DigitalOcean API (POST /v2/droplets).
type DropletSpec struct {
	// name is the hostname of the Droplet. Defaults to metadata.name when empty.
	// +optional
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$`
	Name string `json:"name,omitempty"`

	// region slug where the Droplet is created, e.g. "nyc3", "fra1", "ams3".
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable"
	Region string `json:"region"`

	// size slug of the Droplet, e.g. "s-1vcpu-1gb".
	// +required
	// +kubebuilder:validation:MinLength=1
	Size string `json:"size"`

	// image slug or numeric image ID, e.g. "ubuntu-24-04-x64".
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="image is immutable"
	Image string `json:"image"`

	// sshKeys is a list of SSH key IDs or fingerprints to embed in the Droplet.
	// +optional
	// +listType=set
	SSHKeys []string `json:"sshKeys,omitempty"`

	// backups enables automated backups.
	// +optional
	Backups bool `json:"backups,omitempty"`

	// ipv6 enables IPv6 networking.
	// +optional
	IPv6 bool `json:"ipv6,omitempty"`

	// monitoring installs the DigitalOcean monitoring agent.
	// +optional
	Monitoring bool `json:"monitoring,omitempty"`

	// vpcUUID is the VPC to place the Droplet in. Uses the region default VPC when empty.
	// +optional
	VPCUUID string `json:"vpcUUID,omitempty"`

	// userData is cloud-init data passed to the Droplet on first boot.
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	UserData string `json:"userData,omitempty"`

	// tags applied to the Droplet.
	// +optional
	// +listType=set
	Tags []string `json:"tags,omitempty"`

	// volumes is a list of block storage volume IDs to attach.
	// +optional
	// +listType=set
	Volumes []string `json:"volumes,omitempty"`

	// tokenSecretRef references the Secret holding the DigitalOcean API token.
	// +required
	TokenSecretRef SecretKeyReference `json:"tokenSecretRef"`
}

// DropletStatus defines the observed state of a DigitalOcean Droplet.
type DropletStatus struct {
	// id is the DigitalOcean Droplet ID.
	// +optional
	ID int `json:"id,omitempty"`

	// state reported by DigitalOcean: new, active, off, archive.
	// +optional
	State string `json:"state,omitempty"`

	// publicIPv4 address of the Droplet.
	// +optional
	PublicIPv4 string `json:"publicIPv4,omitempty"`

	// privateIPv4 address of the Droplet.
	// +optional
	PrivateIPv4 string `json:"privateIPv4,omitempty"`

	// publicIPv6 address of the Droplet.
	// +optional
	PublicIPv6 string `json:"publicIPv6,omitempty"`

	// createdAt is when DigitalOcean created the Droplet.
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`

	// observedGeneration is the most recent generation the controller acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Droplet resource.
	// Standard types: "Available", "Progressing", "Degraded".
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=do
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="IP",type=string,JSONPath=`.status.publicIPv4`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Available")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Droplet is a DigitalOcean Droplet managed from Kubernetes
type Droplet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Droplet
	// +required
	Spec DropletSpec `json:"spec"`

	// status defines the observed state of Droplet
	// +optional
	Status DropletStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DropletList contains a list of Droplet
type DropletList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Droplet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Droplet{}, &DropletList{})
		return nil
	})
}
