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
	"fmt"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

// DropletSetReconciler keeps the number of Droplets of a DropletSet equal to
// spec.replicas. It only creates and deletes Droplet objects; the Droplet
// controller creates and deletes the real servers.
type DropletSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=dropletsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=dropletsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=dropletsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets,verbs=get;list;watch;create;update;patch;delete

// Reconcile creates missing Droplets (named <set>-0, <set>-1, ...) and deletes
// the highest-numbered ones when there are too many.
func (r *DropletSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logf.FromContext(ctx)

	set := &webappv1.DropletSet{}
	if err := r.Get(ctx, req.NamespacedName, set); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Kubernetes deletes the child Droplets through their owner references,
	// and each Droplet's finalizer deletes its server.
	if !set.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	desired := 1
	if set.Spec.Replicas != nil {
		desired = int(*set.Spec.Replicas)
	}

	byIndex, extras, err := r.listChildren(ctx, set)
	if err != nil {
		l.Error(err, "Failed to list Droplets of DropletSet")
		return ctrl.Result{}, err
	}

	// Scale up: create every missing index below desired.
	current, ready := 0, 0
	for i := 0; i < desired; i++ {
		d, ok := byIndex[i]
		if ok {
			if !d.DeletionTimestamp.IsZero() {
				// Still being deleted; recreated once it is gone (Owns triggers a reconcile).
				continue
			}
			current++
			if meta.IsStatusConditionTrue(d.Status.Conditions, "Available") {
				ready++
			}
			continue
		}

		d, err := r.newDroplet(set, i)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, d); err != nil {
			if apierrors.IsAlreadyExists(err) {
				err = fmt.Errorf("droplet %q already exists and is not owned by DropletSet %q", d.Name, set.Name)
			}
			l.Error(err, "Failed to create Droplet", "name", d.Name)
			return ctrl.Result{}, err
		}
		l.Info("Created Droplet", "name", d.Name)
		current++
	}

	// Scale down: delete everything at or above desired, highest index first.
	for i := range byIndex {
		if i >= desired {
			extras = append(extras, byIndex[i])
		}
	}
	sort.Slice(extras, func(a, b int) bool { return extras[a].Name > extras[b].Name })
	for _, d := range extras {
		if !d.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, d); client.IgnoreNotFound(err) != nil {
			l.Error(err, "Failed to delete Droplet", "name", d.Name)
			return ctrl.Result{}, err
		}
		l.Info("Deleted Droplet", "name", d.Name)
	}

	return ctrl.Result{}, r.updateStatus(ctx, set, desired, current, ready)
}

// listChildren returns the Droplets this set owns, by index. Owned Droplets
// whose name has no valid index are returned as extras, to be deleted.
func (r *DropletSetReconciler) listChildren(ctx context.Context, set *webappv1.DropletSet) (map[int]*webappv1.Droplet, []*webappv1.Droplet, error) {
	list := &webappv1.DropletList{}
	if err := r.List(ctx, list,
		client.InNamespace(set.Namespace),
		client.MatchingLabels{webappv1.DropletSetLabel: set.Name},
	); err != nil {
		return nil, nil, err
	}

	byIndex := map[int]*webappv1.Droplet{}
	var extras []*webappv1.Droplet
	for i := range list.Items {
		d := &list.Items[i]
		if !metav1.IsControlledBy(d, set) {
			continue
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(d.Name, set.Name+"-"))
		if err != nil || idx < 0 || d.Name != fmt.Sprintf("%s-%d", set.Name, idx) {
			extras = append(extras, d)
			continue
		}
		byIndex[idx] = d
	}
	return byIndex, extras, nil
}

// newDroplet builds the Droplet object for one index of the set.
func (r *DropletSetReconciler) newDroplet(set *webappv1.DropletSet, index int) (*webappv1.Droplet, error) {
	labels := map[string]string{}
	for k, v := range set.Spec.Template.Labels {
		labels[k] = v
	}
	labels[webappv1.DropletSetLabel] = set.Name

	d := &webappv1.Droplet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", set.Name, index),
			Namespace: set.Namespace,
			Labels:    labels,
		},
		Spec: *set.Spec.Template.Spec.DeepCopy(),
	}
	// Each server is named after its object (web-0, web-1), not one shared name.
	d.Spec.Name = ""

	if err := controllerutil.SetControllerReference(set, d, r.Scheme); err != nil {
		return nil, err
	}
	return d, nil
}

// updateStatus writes the counts and conditions to the set's status.
func (r *DropletSetReconciler) updateStatus(ctx context.Context, set *webappv1.DropletSet, desired, current, ready int) error {
	set.Status.Replicas = int32(current)
	set.Status.ReadyReplicas = int32(ready)
	set.Status.Selector = webappv1.DropletSetLabel + "=" + set.Name
	set.Status.ObservedGeneration = set.Generation

	msg := fmt.Sprintf("%d of %d Droplets ready", ready, desired)
	cond := func(t string, s metav1.ConditionStatus, reason string) {
		meta.SetStatusCondition(&set.Status.Conditions, metav1.Condition{
			Type: t, Status: s, Reason: reason, Message: msg, ObservedGeneration: set.Generation,
		})
	}
	if ready == desired {
		cond("Available", metav1.ConditionTrue, "AllReady")
		cond("Progressing", metav1.ConditionFalse, "AllReady")
	} else {
		cond("Available", metav1.ConditionFalse, "Scaling")
		cond("Progressing", metav1.ConditionTrue, "Scaling")
	}

	if err := r.Status().Update(ctx, set); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update DropletSet status")
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager. Owns makes any
// change to a child Droplet (status, deletion) reconcile its DropletSet.
func (r *DropletSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.DropletSet{}).
		Owns(&webappv1.Droplet{}).
		Named("dropletset").
		Complete(r)
}
