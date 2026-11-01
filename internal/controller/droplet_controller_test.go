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
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/digitalocean/godo"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

var _ = Describe("Droplet Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		droplet := &webappv1.Droplet{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Droplet")
			err := k8sClient.Get(ctx, typeNamespacedName, droplet)
			if err != nil && errors.IsNotFound(err) {
				resource := &webappv1.Droplet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: webappv1.DropletSpec{
						Region:         "fra1",
						Size:           "s-1vcpu-1gb",
						Image:          "ubuntu-24-04-x64",
						TokenSecretRef: webappv1.SecretKeyReference{Name: "digitalocean-token"},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &webappv1.Droplet{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Droplet")
			// No controller runs here, so drop the finalizer by hand
			if controllerutil.RemoveFinalizer(resource, dropletFinalizer) {
				Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &DropletReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("adding the finalizer")
			Expect(k8sClient.Get(ctx, typeNamespacedName, droplet)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(droplet, dropletFinalizer)).To(BeTrue())
		})

		It("should create and update a ConfigMap owned by the Droplet", func() {
			controllerReconciler := &DropletReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			Expect(k8sClient.Get(ctx, typeNamespacedName, droplet)).To(Succeed())
			droplet.Status.ID = 42
			droplet.Status.State = "active"
			droplet.Status.PublicIPv4 = "203.0.113.10"

			By("creating the ConfigMap")
			Expect(controllerReconciler.reconcileConfigMap(ctx, droplet)).To(Succeed())

			cm := &corev1.ConfigMap{}
			cmKey := types.NamespacedName{Name: resourceName + "-info", Namespace: resourceNamespace}
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKeyWithValue("id", "42"))
			Expect(cm.Data).To(HaveKeyWithValue("state", "active"))
			Expect(cm.Data).To(HaveKeyWithValue("publicIPv4", "203.0.113.10"))
			Expect(cm.Data).To(HaveKeyWithValue("region", "fra1"))
			Expect(metav1.IsControlledBy(cm, droplet)).To(BeTrue())

			By("updating the ConfigMap when the status changes")
			droplet.Status.State = "off"
			Expect(controllerReconciler.reconcileConfigMap(ctx, droplet)).To(Succeed())
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKeyWithValue("state", "off"))

			// envtest has no garbage collector, so delete it by hand
			Expect(k8sClient.Delete(ctx, cm)).To(Succeed())
		})
	})

	Context("When deleting a Droplet", func() {
		It("should delete the server on DigitalOcean, then let the object go", func() {
			ctx := context.Background()
			key := types.NamespacedName{Name: "delete-test", Namespace: "default"}

			By("starting a fake DigitalOcean API")
			var mu sync.Mutex
			var deleted []string
			fakeDO := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/v2/droplets":
					// A second, untracked server carrying the owner tag
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"droplets":[{"id":456}],"meta":{"total":1}}`))
				case req.Method == http.MethodDelete:
					mu.Lock()
					deleted = append(deleted, req.URL.Path)
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer fakeDO.Close()

			By("creating the token Secret")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "delete-test-token", Namespace: "default"},
				StringData: map[string]string{"token": "fake"},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, secret)).To(Succeed()) })

			By("creating a Droplet with the finalizer and a known ID")
			obj := &webappv1.Droplet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       key.Name,
					Namespace:  key.Namespace,
					Finalizers: []string{dropletFinalizer},
				},
				Spec: webappv1.DropletSpec{
					Region:         "fra1",
					Size:           "s-1vcpu-1gb",
					Image:          "ubuntu-24-04-x64",
					TokenSecretRef: webappv1.SecretKeyReference{Name: secret.Name},
				},
			}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			obj.Status.ID = 123
			Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())

			By("deleting it: the finalizer keeps it around")
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())
			Expect(k8sClient.Get(ctx, key, obj)).To(Succeed())
			Expect(obj.DeletionTimestamp.IsZero()).To(BeFalse())

			By("reconciling")
			r := &DropletReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				NewDOClient: func(string) *godo.Client {
					c, err := godo.New(fakeDO.Client(), godo.SetBaseURL(fakeDO.URL+"/"))
					Expect(err).NotTo(HaveOccurred())
					return c
				},
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			Expect(deleted).To(ConsistOf("/v2/droplets/123", "/v2/droplets/456"))
			err = k8sClient.Get(ctx, key, obj)
			Expect(errors.IsNotFound(err)).To(BeTrue(), "object should be gone once the finalizer is removed")
		})
	})
})
