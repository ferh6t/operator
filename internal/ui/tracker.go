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

// Package ui serves a small live dashboard for the Droplet reconciler.
package ui

import (
	"sync"
	"time"
)

// maxEvents is how many recent reconciles the dashboard keeps in memory.
const maxEvents = 200

// Event is one finished Reconcile call.
type Event struct {
	Time       time.Time     `json:"time"`
	Namespace  string        `json:"namespace"`
	Name       string        `json:"name"`
	Duration   time.Duration `json:"durationNs"`
	Error      string        `json:"error,omitempty"`
	RequeueIn  time.Duration `json:"requeueInNs,omitempty"`
	Generation int64         `json:"generation"`
}

// Tracker records recent reconciles. It is safe for concurrent use, and a nil
// Tracker ignores all calls so tests can leave it out.
type Tracker struct {
	mu     sync.RWMutex
	events []Event
	last   map[string]Event
	total  int64
	errors int64
}

// NewTracker returns an empty Tracker.
func NewTracker() *Tracker {
	return &Tracker{last: map[string]Event{}}
}

// Record stores a finished reconcile.
func (t *Tracker) Record(e Event) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.total++
	if e.Error != "" {
		t.errors++
	}
	t.last[e.Namespace+"/"+e.Name] = e
	t.events = append(t.events, e)
	if len(t.events) > maxEvents {
		t.events = t.events[len(t.events)-maxEvents:]
	}
}

// snapshot returns copies of the recorded data, newest event first.
func (t *Tracker) snapshot() (events []Event, last map[string]Event, total, errors int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	events = make([]Event, len(t.events))
	for i, e := range t.events {
		events[len(t.events)-1-i] = e
	}
	last = make(map[string]Event, len(t.last))
	for k, v := range t.last {
		last[k] = v
	}
	return events, last, t.total, t.errors
}
