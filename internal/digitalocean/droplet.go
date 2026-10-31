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

// Package digitalocean talks to the DigitalOcean API for the operator.
package digitalocean

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/digitalocean/godo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

// ErrNotFound means the Droplet does not exist in DigitalOcean.
var ErrNotFound = errors.New("droplet not found in DigitalOcean")

// NewClient returns a DigitalOcean API client that uses the given token.
func NewClient(token string) *godo.Client {
	return godo.NewFromToken(token)
}

// OwnerTag is a tag put on every Droplet the operator creates. It ties the
// real server to one Kubernetes object, so the operator can find it again
// even if writing the ID into status failed.
func OwnerTag(droplet *webappv1.Droplet) string {
	return "k8s-uid:" + string(droplet.UID)
}

// DropletName returns spec.name, or metadata.name when spec.name is empty.
func DropletName(droplet *webappv1.Droplet) string {
	if droplet.Spec.Name != "" {
		return droplet.Spec.Name
	}
	return droplet.Name
}

// Ensure returns the DigitalOcean Droplet that belongs to this object,
// creating it if it does not exist yet. It is safe to call on every reconcile.
func Ensure(ctx context.Context, client *godo.Client, droplet *webappv1.Droplet) (*godo.Droplet, bool, error) {
	// 1. We already know its ID.
	if droplet.Status.ID != 0 {
		d, err := Get(ctx, client, droplet.Status.ID)
		if err == nil {
			return d, false, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, false, err
		}
		// Deleted outside Kubernetes; fall through and look it up by tag.
	}

	// 2. It exists but its ID never reached status.
	d, err := FindByOwnerTag(ctx, client, OwnerTag(droplet))
	if err == nil {
		return d, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}

	// 3. It does not exist: create it.
	d, err = Create(ctx, client, droplet)
	if err != nil {
		return nil, false, err
	}
	return d, true, nil
}

// Create creates a new Droplet in DigitalOcean from the object's spec.
func Create(ctx context.Context, client *godo.Client, droplet *webappv1.Droplet) (*godo.Droplet, error) {
	req := BuildCreateRequest(droplet)
	d, _, err := client.Droplets.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating droplet %q: %w", req.Name, err)
	}
	return d, nil
}

// Get fetches a Droplet by ID. It returns ErrNotFound when it does not exist.
func Get(ctx context.Context, client *godo.Client, id int) (*godo.Droplet, error) {
	d, resp, err := client.Droplets.Get(ctx, id)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("getting droplet %d: %w", id, err)
	}
	return d, nil
}

// FindByOwnerTag returns the Droplet carrying the given tag, or ErrNotFound.
func FindByOwnerTag(ctx context.Context, client *godo.Client, tag string) (*godo.Droplet, error) {
	list, _, err := client.Droplets.ListByTag(ctx, tag, &godo.ListOptions{PerPage: 10})
	if err != nil {
		return nil, fmt.Errorf("listing droplets with tag %q: %w", tag, err)
	}
	if len(list) == 0 {
		return nil, ErrNotFound
	}
	return &list[0], nil
}

// BuildCreateRequest turns the object's spec into a DigitalOcean create request.
func BuildCreateRequest(droplet *webappv1.Droplet) *godo.DropletCreateRequest {
	spec := droplet.Spec

	// image can be a slug ("ubuntu-24-04-x64") or a numeric image ID.
	image := godo.DropletCreateImage{Slug: spec.Image}
	if id, err := strconv.Atoi(spec.Image); err == nil {
		image = godo.DropletCreateImage{ID: id}
	}

	// sshKeys can be fingerprints or numeric key IDs.
	sshKeys := make([]godo.DropletCreateSSHKey, 0, len(spec.SSHKeys))
	for _, k := range spec.SSHKeys {
		if id, err := strconv.Atoi(k); err == nil {
			sshKeys = append(sshKeys, godo.DropletCreateSSHKey{ID: id})
		} else {
			sshKeys = append(sshKeys, godo.DropletCreateSSHKey{Fingerprint: k})
		}
	}

	volumes := make([]godo.DropletCreateVolume, 0, len(spec.Volumes))
	for _, v := range spec.Volumes {
		volumes = append(volumes, godo.DropletCreateVolume{ID: v})
	}

	tags := append([]string{OwnerTag(droplet)}, spec.Tags...)

	return &godo.DropletCreateRequest{
		Name:       DropletName(droplet),
		Region:     spec.Region,
		Size:       spec.Size,
		Image:      image,
		SSHKeys:    sshKeys,
		Backups:    spec.Backups,
		IPv6:       spec.IPv6,
		Monitoring: spec.Monitoring,
		UserData:   spec.UserData,
		Volumes:    volumes,
		Tags:       tags,
		VPCUUID:    spec.VPCUUID,
	}
}

// ApplyStatus copies what DigitalOcean reports into the object's status.
func ApplyStatus(droplet *webappv1.Droplet, d *godo.Droplet) {
	droplet.Status.ID = d.ID
	droplet.Status.State = d.Status

	// Errors here only mean the address is not assigned yet.
	droplet.Status.PublicIPv4, _ = d.PublicIPv4()
	droplet.Status.PrivateIPv4, _ = d.PrivateIPv4()
	droplet.Status.PublicIPv6, _ = d.PublicIPv6()

	if t, err := time.Parse(time.RFC3339, d.Created); err == nil {
		created := metav1.NewTime(t)
		droplet.Status.CreatedAt = &created
	}
}
