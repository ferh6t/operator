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

package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

func TestHandleState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := webappv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&webappv1.Droplet{
			ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"},
			Spec:       webappv1.DropletSpec{Region: "fra1"},
			Status:     webappv1.DropletStatus{ID: 42, State: "active", PublicIPv4: "203.0.113.7"},
		},
		&webappv1.Droplet{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "default"}},
	).Build()

	tr := NewTracker()
	tr.Record(Event{Time: time.Now(), Namespace: "default", Name: "b", Duration: time.Millisecond})
	tr.Record(Event{Time: time.Now(), Namespace: "default", Name: "b", Error: "boom"})

	s := &Server{Reader: c, Tracker: tr}
	rec := httptest.NewRecorder()
	s.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))

	var got stateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Droplets) != 2 || got.Droplets[0].Name != "a" || got.Droplets[1].Name != "b" {
		t.Fatalf("droplets not listed in order: %+v", got.Droplets)
	}
	b := got.Droplets[1]
	if b.Status.PublicIPv4 != "203.0.113.7" || b.LastReconcile == nil || b.LastReconcile.Error != "boom" {
		t.Fatalf("droplet b missing status or last reconcile: %+v", b)
	}
	if got.Total != 2 || got.Errors != 1 || len(got.Events) != 2 || got.Events[0].Error != "boom" {
		t.Fatalf("wrong totals or event order: total=%d errors=%d events=%+v", got.Total, got.Errors, got.Events)
	}
}

func TestTrackerKeepsRecentEvents(t *testing.T) {
	tr := NewTracker()
	for i := 0; i < maxEvents+50; i++ {
		tr.Record(Event{Name: "x"})
	}
	events, _, total, _ := tr.snapshot()
	if len(events) != maxEvents || total != int64(maxEvents+50) {
		t.Fatalf("got %d events, total %d", len(events), total)
	}
	var nilTracker *Tracker
	nilTracker.Record(Event{}) // must not panic
}
