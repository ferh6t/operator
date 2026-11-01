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
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

var _ = Describe("DropletSet Controller", func() {
	It("should scale its Droplets up and down", func() {
		ctx := context.Background()
		key := types.NamespacedName{Name: "web", Namespace: "default"}
		r := &DropletSetReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		reconcileSet := func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}
		childNames := func() []string {
			list := &webappv1.DropletList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(key.Namespace),
				client.MatchingLabels{webappv1.DropletSetLabel: key.Name})).To(Succeed())
			var names []string
			for _, d := range list.Items {
				if d.DeletionTimestamp.IsZero() {
					names = append(names, d.Name)
				}
			}
			sort.Strings(names)
			return names
		}
		scaleTo := func(n int32) {
			set := &webappv1.DropletSet{}
			Expect(k8sClient.Get(ctx, key, set)).To(Succeed())
			set.Spec.Replicas = ptr.To(n)
			Expect(k8sClient.Update(ctx, set)).To(Succeed())
		}

		By("creating a DropletSet with 3 replicas")
		set := &webappv1.DropletSet{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Spec: webappv1.DropletSetSpec{
				Replicas: ptr.To(int32(3)),
				Template: webappv1.DropletTemplate{
					Labels: map[string]string{"app": "web"},
					Spec: webappv1.DropletSpec{
						Name:           "ignored",
						Region:         "fra1",
						Size:           "s-1vcpu-1gb",
						Image:          "ubuntu-24-04-x64",
						TokenSecretRef: webappv1.SecretKeyReference{Name: "digitalocean-token"},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, set)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, set)).To(Succeed()) })

		reconcileSet()
		Expect(childNames()).To(Equal([]string{"web-0", "web-1", "web-2"}))

		child := &webappv1.Droplet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "web-1", Namespace: "default"}, child)).To(Succeed())
		Expect(metav1.IsControlledBy(child, set)).To(BeTrue())
		Expect(child.Labels).To(HaveKeyWithValue("app", "web"))
		Expect(child.Spec.Name).To(BeEmpty(), "each server is named after its object")
		Expect(child.Spec.Size).To(Equal("s-1vcpu-1gb"))

		Expect(k8sClient.Get(ctx, key, set)).To(Succeed())
		Expect(set.Status.Replicas).To(Equal(int32(3)))
		Expect(set.Status.ReadyReplicas).To(Equal(int32(0)))
		Expect(set.Status.Selector).To(Equal(webappv1.DropletSetLabel + "=web"))

		By("counting a Droplet as ready once it is Available")
		meta.SetStatusCondition(&child.Status.Conditions, metav1.Condition{
			Type: "Available", Status: metav1.ConditionTrue, Reason: "Active", Message: "Droplet is running",
		})
		Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())
		reconcileSet()
		Expect(k8sClient.Get(ctx, key, set)).To(Succeed())
		Expect(set.Status.ReadyReplicas).To(Equal(int32(1)))

		By("scaling down to 1: the highest numbers go first")
		scaleTo(1)
		reconcileSet()
		Expect(childNames()).To(Equal([]string{"web-0"}))

		By("scaling up to 2: the missing number comes back")
		scaleTo(2)
		reconcileSet()
		Expect(childNames()).To(Equal([]string{"web-0", "web-1"}))

		By("scaling to 0")
		scaleTo(0)
		reconcileSet()
		Expect(childNames()).To(BeEmpty())
		Expect(k8sClient.Get(ctx, key, set)).To(Succeed())
		Expect(set.Status.Replicas).To(Equal(int32(0)))
		Expect(meta.IsStatusConditionTrue(set.Status.Conditions, "Available")).To(BeTrue())
	})
})
