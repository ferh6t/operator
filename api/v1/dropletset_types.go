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

// DropletSetLabel is put on every Droplet a DropletSet creates. Its value is
// the DropletSet name. It is also the selector `kubectl scale` reports.
const DropletSetLabel = "webapp.ferh6t.github.io/dropletset"

// DropletTemplate describes the Droplets a DropletSet creates.
type DropletTemplate struct {
	// labels added to every Droplet created from this template.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// spec of every Droplet. spec.name is ignored: each Droplet is named
	// <dropletset-name>-<index>, e.g. web-0, web-1.
	// +required
	Spec DropletSpec `json:"spec"`
}

// DropletSetSpec defines how many Droplets to run and what they look like.
type DropletSetSpec struct {
	// replicas is the number of Droplets to run. Scaling down deletes the
	// highest-numbered Droplets first.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// template for the Droplets. Changes only apply to Droplets created later.
	// +required
	Template DropletTemplate `json:"template"`
}

// DropletSetStatus defines the observed state of DropletSet.
type DropletSetStatus struct {
	// replicas is the number of Droplets that exist and are not being deleted.
	// +optional
	Replicas int32 `json:"replicas"`

	// readyReplicas is the number of Droplets whose Available condition is True.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// selector is the label selector of the Droplets, used by `kubectl scale`
	// and autoscalers.
	// +optional
	Selector string `json:"selector,omitempty"`

	// observedGeneration is the most recent generation the controller acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the DropletSet.
	// Standard types: "Available", "Progressing".
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:scope=Namespaced,shortName=dos
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.template.spec.size`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DropletSet keeps a number of identical DigitalOcean Droplets running,
// like a ReplicaSet does for Pods.
type DropletSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DropletSet
	// +required
	Spec DropletSetSpec `json:"spec"`

	// status defines the observed state of DropletSet
	// +optional
	Status DropletSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DropletSetList contains a list of DropletSet
type DropletSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DropletSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DropletSet{}, &DropletSetList{})
		return nil
	})
}
