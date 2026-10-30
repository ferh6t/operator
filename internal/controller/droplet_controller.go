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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

// DropletReconciler reconciles a Droplet object
type DropletReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=webapp.ferh6t.github.io,resources=droplets/finalizers,verbs=update

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

	fmt.Println("I got request namespace:", req.Namespace, "name:", req.Name)
	fmt.Println("droplet instance name:", dropletObject.Name)
	fmt.Println("droplet namespace:", dropletObject.Namespace)
	fmt.Println("droplet generation:", dropletObject.Generation)
	fmt.Println("droplet labels:", dropletObject.Labels)
	fmt.Printf("droplet spec: %+v\n", dropletObject.Spec)
	fmt.Printf("droplet status: %+v\n", dropletObject.Status)

	// Check the Droplet again every 10 seconds, even without changes
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DropletReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.Droplet{}).
		Named("droplet").
		Complete(r)
}
