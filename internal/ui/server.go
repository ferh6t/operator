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
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	webappv1 "github.com/ferh6t/operator/api/v1"
)

//go:embed index.html
var indexHTML []byte

// Server serves the dashboard. It is added to the manager as a Runnable, so it
// starts and stops together with the controllers.
type Server struct {
	Addr    string
	Reader  client.Reader
	Tracker *Tracker
}

// dropletView is one row in the dashboard.
type dropletView struct {
	Namespace     string                 `json:"namespace"`
	Name          string                 `json:"name"`
	Generation    int64                  `json:"generation"`
	CreatedAt     time.Time              `json:"createdAt"`
	Deleting      bool                   `json:"deleting"`
	Labels        map[string]string      `json:"labels,omitempty"`
	Spec          webappv1.DropletSpec   `json:"spec"`
	Status        webappv1.DropletStatus `json:"status"`
	LastReconcile *Event                 `json:"lastReconcile,omitempty"`
}

type stateResponse struct {
	Now      time.Time     `json:"now"`
	Droplets []dropletView `json:"droplets"`
	Events   []Event       `json:"events"`
	Total    int64         `json:"totalReconciles"`
	Errors   int64         `json:"failedReconciles"`
	ListErr  string        `json:"listError,omitempty"`
}

// NeedLeaderElection lets the dashboard run on every replica, not only the leader.
func (s *Server) NeedLeaderElection() bool { return false }

// Start runs the HTTP server until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("ui")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/state", s.handleState)

	srv := &http.Server{Addr: s.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("Starting dashboard", "address", s.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	events, last, total, failed := s.Tracker.snapshot()
	resp := stateResponse{Now: time.Now(), Events: events, Total: total, Errors: failed, Droplets: []dropletView{}}

	list := &webappv1.DropletList{}
	if err := s.Reader.List(r.Context(), list); err != nil {
		resp.ListErr = err.Error()
	}
	for _, d := range list.Items {
		v := dropletView{
			Namespace:  d.Namespace,
			Name:       d.Name,
			Generation: d.Generation,
			CreatedAt:  d.CreationTimestamp.Time,
			Deleting:   d.DeletionTimestamp != nil,
			Labels:     d.Labels,
			Spec:       d.Spec,
			Status:     d.Status,
		}
		// The API token is never sent: the spec only holds the Secret's name.
		if e, ok := last[d.Namespace+"/"+d.Name]; ok {
			v.LastReconcile = &e
		}
		resp.Droplets = append(resp.Droplets, v)
	}
	sort.Slice(resp.Droplets, func(i, j int) bool {
		a, b := resp.Droplets[i], resp.Droplets[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}
