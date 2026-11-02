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
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

// The primary proxy lets clients inside Kubernetes, or on a laptop through
// `kubectl port-forward`, reach the primary without opening PostgreSQL to the
// internet. A pod holds an SSH tunnel to the primary, and the Service
// <cluster>-primary points at it. After a failover the controller points the
// tunnel at the new primary.
const (
	// proxyImage only needs a shell and apk to install the SSH client.
	proxyImage = "alpine:3.20.3"
	// proxyAppLabel marks the proxy pods.
	proxyAppLabel = "postgres-primary-proxy"
)

// proxyScript checks the primary's host key against the fingerprint the
// controller pinned, then forwards port 5432 to PostgreSQL on the primary.
// ssh exits when the tunnel breaks, and the pod restarts.
const proxyScript = `set -eu
apk add --no-cache openssh-client >/dev/null
known=/tmp/known_hosts
: > "$known"
ssh-keyscan -T 10 "$HOST" 2>/dev/null | grep -v '^#' | while read -r line; do
  fp=$(echo "$line" | ssh-keygen -lf - 2>/dev/null | awk '{print $2}')
  if [ "$fp" = "$FINGERPRINT" ]; then echo "$line" >> "$known"; fi
done
if [ ! -s "$known" ]; then
  echo "$PRIMARY ($HOST) did not present the pinned host key $FINGERPRINT" >&2
  sleep 10
  exit 1
fi
echo "Forwarding port 5432 to PostgreSQL on $PRIMARY ($HOST)"
exec ssh -N -i /ssh/id -o UserKnownHostsFile="$known" -o StrictHostKeyChecking=yes \
  -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
  -L 0.0.0.0:5432:127.0.0.1:5432 "root@$HOST"
`

// primaryServiceName is the Service clients connect to.
func primaryServiceName(pg *webappv1.PostgresCluster) string { return pg.Name + "-primary" }

// proxyName is the Deployment that runs the tunnel.
func proxyName(pg *webappv1.PostgresCluster) string { return pg.Name + "-primary-proxy" }

// proxyLabels select the proxy pods of one cluster.
func proxyLabels(pg *webappv1.PostgresCluster) map[string]string {
	return map[string]string{"app.kubernetes.io/name": proxyAppLabel, webappv1.PostgresClusterLabel: pg.Name}
}

// ensurePrimaryProxy points the proxy at the current primary. Without a known
// primary it keeps the last one, so a short election does not restart it.
func (r *PostgresClusterReconciler) ensurePrimaryProxy(ctx context.Context, pg *webappv1.PostgresCluster) error {
	idx := slices.IndexFunc(pg.Status.Members, func(m webappv1.PostgresMember) bool { return m.Name == pg.Status.Primary })
	if pg.Status.Primary == "" || idx < 0 {
		return nil
	}
	primary := pg.Status.Members[idx]
	if primary.PublicIPv4 == "" || primary.HostKey == "" {
		return nil
	}
	if err := r.ensureProxyDeployment(ctx, pg, primary); err != nil {
		return err
	}
	return r.ensurePrimaryService(ctx, pg)
}

// ensureProxyDeployment creates the proxy, or changes its target when the
// primary moved. Changing the target rolls the pod.
func (r *PostgresClusterReconciler) ensureProxyDeployment(ctx context.Context, pg *webappv1.PostgresCluster, primary webappv1.PostgresMember) error {
	env := []corev1.EnvVar{
		{Name: "PRIMARY", Value: primary.Name},
		{Name: "HOST", Value: primary.PublicIPv4},
		{Name: "FINGERPRINT", Value: primary.HostKey},
	}

	dep := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKey{Namespace: pg.Namespace, Name: proxyName(pg)}, dep)
	if apierrors.IsNotFound(err) {
		dep = newProxyDeployment(pg, env)
		if err := controllerutil.SetControllerReference(pg, dep, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, dep); err != nil {
			return err
		}
		logf.FromContext(ctx).Info("Created primary proxy", "name", dep.Name, "primary", primary.Name)
		return nil
	}
	if err != nil {
		return err
	}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) == 1 && slices.Equal(containers[0].Env, env) {
		return nil
	}
	want := newProxyDeployment(pg, env)
	dep.Spec.Template = want.Spec.Template
	if err := r.Update(ctx, dep); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Pointed primary proxy at new primary", "name", dep.Name, "primary", primary.Name)
	return nil
}

// newProxyDeployment builds the proxy Deployment. Recreate keeps at most one
// tunnel, so clients never reach the old primary through a second pod.
func newProxyDeployment(pg *webappv1.PostgresCluster, env []corev1.EnvVar) *appsv1.Deployment {
	labels := proxyLabels(pg)
	replicas := int32(1)
	keyMode := int32(0o400)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: proxyName(pg), Namespace: pg.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:    "tunnel",
						Image:   proxyImage,
						Command: []string{"/bin/sh", "-c", proxyScript},
						Env:     env,
						Ports:   []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432, Protocol: corev1.ProtocolTCP}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("postgres")}},
							PeriodSeconds: 5,
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "ssh", MountPath: "/ssh", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{
						Name: "ssh",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName:  pg.Name + "-ssh",
							DefaultMode: &keyMode,
							Items:       []corev1.KeyToPath{{Key: corev1.SSHAuthPrivateKey, Path: "id"}},
						}},
					}},
				},
			},
		},
	}
}

// ensurePrimaryService creates the Service <cluster>-primary on port 5432.
func (r *PostgresClusterReconciler) ensurePrimaryService(ctx context.Context, pg *webappv1.PostgresCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: primaryServiceName(pg), Namespace: pg.Namespace}}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = proxyLabels(pg)
		svc.Spec.Selector = proxyLabels(pg)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name: "postgres", Port: 5432, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("postgres"),
		}}
		return controllerutil.SetControllerReference(pg, svc, r.Scheme)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated {
		logf.FromContext(ctx).Info("Created primary Service", "name", svc.Name)
	}
	return nil
}
