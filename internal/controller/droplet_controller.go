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
	"strconv"
	"time"

	"github.com/digitalocean/godo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
	"github.com/ferh6t/operator/internal/digitalocean"
	"github.com/ferh6t/operator/internal/ui"
)

// requeueInterval is how often a Droplet is checked again, even without changes.
const requeueInterval = 20 * time.Second

// dropletFinalizer stops Kubernetes from removing a Droplet object until the
// controller has deleted the real server on DigitalOcean.
const dropletFinalizer = "webapp.ferh6t.github.io/droplet-cleanup"

// DropletReconciler reconciles a Droplet object
type DropletReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Tracker records each reconcile for the dashboard. It may be nil.
	Tracker *ui.Tracker

	// NewDOClient builds the DigitalOcean client from a token. Tests replace
	// it with a fake. Nil means digitalocean.NewClient.
	NewDOClient func(token string) *godo.Client
}

// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Droplet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *DropletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	result, err := r.reconcile(ctx, req)
	r.record(ctx, req, start, result, err)
	return result, err
}

// record sends the outcome of one reconcile to the dashboard.
func (r *DropletReconciler) record(ctx context.Context, req ctrl.Request, start time.Time, result ctrl.Result, err error) {
	if r.Tracker == nil {
		return
	}
	e := ui.Event{
		Time:      time.Now(),
		Namespace: req.Namespace,
		Name:      req.Name,
		Duration:  time.Since(start),
		RequeueIn: result.RequeueAfter,
	}
	if err != nil {
		e.Error = err.Error()
	}
	d := &webappv1.Droplet{}
	if r.Get(ctx, req.NamespacedName, d) == nil {
		e.Generation = d.Generation
		// A Degraded condition is a failure even when no error was returned.
		if c := meta.FindStatusCondition(d.Status.Conditions, "Degraded"); e.Error == "" && c != nil && c.Status == metav1.ConditionTrue {
			e.Error = c.Message
		}
	}
	r.Tracker.Record(e)
}

// reconcile does the actual work for one Droplet.
func (r *DropletReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logf.FromContext(ctx)
	l.Info("Reconciling Droplet")

	dropletObject := &webappv1.Droplet{}
	if err := r.Get(ctx, req.NamespacedName, dropletObject); err != nil {
		if apierrors.IsNotFound(err) {
			l.Info("Droplet not found, ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		l.Error(err, "Failed to get Droplet")
		return ctrl.Result{}, err
	}

	// The object is being deleted: clean up DigitalOcean first.
	if !dropletObject.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, dropletObject)
	}

	// Add the finalizer before anything can be created on DigitalOcean.
	if controllerutil.AddFinalizer(dropletObject, dropletFinalizer) {
		if err := r.Update(ctx, dropletObject); err != nil {
			l.Error(err, "Failed to add finalizer to Droplet")
			return ctrl.Result{}, err
		}
	}

	token, err := r.readToken(ctx, dropletObject)
	if err != nil {
		l.Error(err, "Could not read DigitalOcean token")
		setCondition(dropletObject, "Degraded", metav1.ConditionTrue, "TokenUnavailable", err.Error())
		if uerr := r.Status().Update(ctx, dropletObject); uerr != nil {
			return ctrl.Result{}, uerr
		}
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	doClient := r.doClient(token)
	d, created, err := digitalocean.Ensure(ctx, doClient, dropletObject)
	if err != nil {
		l.Error(err, "Could not create or find DigitalOcean Droplet")
		setCondition(dropletObject, "Degraded", metav1.ConditionTrue, "DigitalOceanError", err.Error())
		if uerr := r.Status().Update(ctx, dropletObject); uerr != nil {
			return ctrl.Result{}, uerr
		}
		return ctrl.Result{}, err
	}
	if created {
		l.Info("Created DigitalOcean Droplet", "id", d.ID, "name", d.Name)
	}

	digitalocean.ApplyStatus(dropletObject, d)
	dropletObject.Status.ObservedGeneration = dropletObject.Generation
	meta.RemoveStatusCondition(&dropletObject.Status.Conditions, "Degraded")
	if d.Status == "active" {
		setCondition(dropletObject, "Available", metav1.ConditionTrue, "Active", "Droplet is running")
		setCondition(dropletObject, "Progressing", metav1.ConditionFalse, "Active", "Droplet is running")
	} else {
		setCondition(dropletObject, "Available", metav1.ConditionFalse, "Provisioning", "Droplet state is "+d.Status)
		setCondition(dropletObject, "Progressing", metav1.ConditionTrue, "Provisioning", "Droplet state is "+d.Status)
	}

	if err := r.Status().Update(ctx, dropletObject); err != nil {
		l.Error(err, "Failed to update Droplet status")
		return ctrl.Result{}, err
	}

	if err := r.reconcileConfigMap(ctx, dropletObject); err != nil {
		l.Error(err, "Failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// Check the Droplet again every 20 seconds, even without changes
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// finalize deletes the real server on DigitalOcean, then removes the
// finalizer so Kubernetes can delete the object. On failure the finalizer
// stays and the next reconcile tries again.
func (r *DropletReconciler) finalize(ctx context.Context, droplet *webappv1.Droplet) (ctrl.Result, error) {
	l := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(droplet, dropletFinalizer) {
		return ctrl.Result{}, nil
	}

	token, err := r.readToken(ctx, droplet)
	if err != nil {
		l.Error(err, "Could not read DigitalOcean token, so the DigitalOcean Droplet was not deleted")
		return r.failDeletion(ctx, droplet, "TokenUnavailable", err)
	}

	if err := digitalocean.Delete(ctx, r.doClient(token), droplet); err != nil {
		l.Error(err, "Could not delete DigitalOcean Droplet", "id", droplet.Status.ID)
		return r.failDeletion(ctx, droplet, "DeletionFailed", err)
	}
	l.Info("Deleted DigitalOcean Droplet", "id", droplet.Status.ID)

	controllerutil.RemoveFinalizer(droplet, dropletFinalizer)
	if err := r.Update(ctx, droplet); err != nil {
		l.Error(err, "Failed to remove finalizer from Droplet")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// failDeletion records why cleanup failed and returns the error, so the
// controller retries with backoff.
func (r *DropletReconciler) failDeletion(ctx context.Context, droplet *webappv1.Droplet, reason string, err error) (ctrl.Result, error) {
	setCondition(droplet, "Degraded", metav1.ConditionTrue, reason, "Could not delete Droplet from DigitalOcean: "+err.Error())
	if uerr := r.Status().Update(ctx, droplet); uerr != nil {
		return ctrl.Result{}, uerr
	}
	return ctrl.Result{}, err
}

// doClient returns a DigitalOcean client for the token.
func (r *DropletReconciler) doClient(token string) *godo.Client {
	if r.NewDOClient != nil {
		return r.NewDOClient(token)
	}
	return digitalocean.NewClient(token)
}

// configMapName is the name of the ConfigMap that holds a Droplet's details.
func configMapName(droplet *webappv1.Droplet) string {
	return droplet.Name + "-info"
}

// reconcileConfigMap creates or updates a ConfigMap with the Droplet's
// address and state, so other apps in the cluster can read them.
// The Droplet owns it, so Kubernetes deletes it together with the Droplet.
func (r *DropletReconciler) reconcileConfigMap(ctx context.Context, droplet *webappv1.Droplet) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(droplet),
			Namespace: droplet.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"id":          strconv.Itoa(droplet.Status.ID),
			"state":       droplet.Status.State,
			"publicIPv4":  droplet.Status.PublicIPv4,
			"privateIPv4": droplet.Status.PrivateIPv4,
			"publicIPv6":  droplet.Status.PublicIPv6,
			"region":      droplet.Spec.Region,
			"size":        droplet.Spec.Size,
		}
		return controllerutil.SetControllerReference(droplet, cm, r.Scheme)
	})
	if err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled ConfigMap", "name", cm.Name, "operation", op)
	}
	return nil
}

// readToken reads the DigitalOcean API token from the Secret in spec.tokenSecretRef.
func (r *DropletReconciler) readToken(ctx context.Context, droplet *webappv1.Droplet) (string, error) {
	ref := droplet.Spec.TokenSecretRef
	key := ref.Key
	if key == "" {
		key = "token"
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: droplet.Namespace, Name: ref.Name}, secret); err != nil {
		return "", fmt.Errorf("getting Secret %q: %w", ref.Name, err)
	}
	token := string(secret.Data[key])
	if token == "" {
		return "", fmt.Errorf("secret %q has no key %q", ref.Name, key)
	}
	return token, nil
}

// setCondition adds or updates one status condition.
func setCondition(droplet *webappv1.Droplet, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&droplet.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: droplet.Generation,
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *DropletReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.Droplet{}).
		Owns(&corev1.ConfigMap{}).
		Named("droplet").
		Complete(r)
}
