// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package most

import (
	"context"
	"sync"
	"time"
)

type flightResult struct {
	data     []byte
	fromS3   bool
	fromNVMe bool
}

type flightCall struct {
	done    chan struct{}
	res     flightResult
	err     error
	waiters int
	cancel  context.CancelFunc
}

// singleFlight coalesces concurrent backend reads for the same key using a
// detached fetch context so that cancellation of the initiating caller never
// aborts other live waiters. When all waiters have abandoned the call, the
// detached context is canceled promptly to prevent goroutine leaks.
type singleFlight struct {
	mu      sync.Mutex
	calls   map[string]*flightCall
	timeout time.Duration
}

func newSingleFlight(timeout time.Duration) *singleFlight {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &singleFlight{
		calls:   make(map[string]*flightCall),
		timeout: timeout,
	}
}

// Do executes fn at most once concurrently for key. It returns the result,
// whether this caller coalesced onto an existing flight, and any error.
func (g *singleFlight) Do(ctx context.Context, key string, fn func(context.Context) (flightResult, error)) (res flightResult, coalesced bool, err error) {
	if err := ctx.Err(); err != nil {
		return flightResult{}, false, err
	}

	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		c.waiters++
		g.mu.Unlock()
		return g.await(ctx, key, c, true)
	}

	flightCtx, cancel := context.WithTimeout(context.Background(), g.timeout)
	c := &flightCall{
		done:    make(chan struct{}),
		waiters: 1,
		cancel:  cancel,
	}
	g.calls[key] = c
	g.mu.Unlock()

	go func() {
		defer cancel()
		r, fnErr := fn(flightCtx)
		g.mu.Lock()
		c.res = r
		c.err = fnErr
		if g.calls[key] == c {
			delete(g.calls, key)
		}
		close(c.done)
		g.mu.Unlock()
	}()

	return g.await(ctx, key, c, false)
}

func (g *singleFlight) await(ctx context.Context, key string, c *flightCall, coalesced bool) (flightResult, bool, error) {
	select {
	case <-c.done:
		return c.res, coalesced, c.err
	case <-ctx.Done():
		g.mu.Lock()
		c.waiters--
		shouldCancel := c.waiters <= 0
		if shouldCancel {
			// Remove from map if still active so a new caller starts fresh.
			if g.calls[key] == c {
				delete(g.calls, key)
			}
		}
		g.mu.Unlock()
		if shouldCancel {
			c.cancel()
		}
		return flightResult{}, coalesced, ctx.Err()
	}
}
