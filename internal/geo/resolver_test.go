package geo

import (
	"context"
	"net"
	"sync"
	"testing"
)

func TestNoopProvider(t *testing.T) {
	var p Provider = NoopProvider{}
	if !p.Lookup(net.ParseIP("8.8.8.8")).IsZero() {
		t.Error("noop lookup should be zero")
	}
	if p.Name() != "noop" {
		t.Errorf("noop name = %q; want noop", p.Name())
	}
	if err := p.Close(); err != nil {
		t.Errorf("noop close: %v", err)
	}
}

func TestNewResolverNilFallsBackToNoop(t *testing.T) {
	r := NewResolver(nil)
	if r.ProviderName() != "noop" {
		t.Errorf("nil provider → %q; want noop", r.ProviderName())
	}
	if !r.Resolve("8.8.8.8").IsZero() {
		t.Error("nil→noop resolve should be zero")
	}
}

func TestSetupEmptyPathIsNoop(t *testing.T) {
	r := Setup(context.Background(), Config{})
	if r.ProviderName() != "noop" {
		t.Errorf("empty path → %q; want noop", r.ProviderName())
	}
	if !r.Resolve("176.57.218.121").IsZero() {
		t.Error("noop resolve should be zero")
	}
}

func TestSetupMissingFileNoURLIsNoop(t *testing.T) {
	// A configured but broken path with no download URL must degrade to noop, never error.
	r := Setup(context.Background(), Config{Path: "/nonexistent/does-not-exist.mmdb"})
	if r.ProviderName() != "noop" {
		t.Errorf("broken path → %q; want noop", r.ProviderName())
	}
}

func TestResolvePrivateAndGarbageNoPanic(t *testing.T) {
	r := NewResolver(NoopProvider{})
	for _, s := range []string{"127.0.0.1", "10.0.0.1", "192.168.1.1", "::1", "", "garbage", "1.2.3.4:80"} {
		if !r.Resolve(s).IsZero() {
			t.Errorf("Resolve(%q) should be zero under noop", s)
		}
	}
}

func TestSwapProviderIgnoresNil(t *testing.T) {
	r := NewResolver(NoopProvider{})
	r.SwapProvider(nil)
	if r.ProviderName() != "noop" {
		t.Errorf("SwapProvider(nil) changed provider to %q", r.ProviderName())
	}
}

// TestResolverRace: run with -race. Concurrent Resolve + SwapProvider must be data-race-free.
func TestResolverRace(t *testing.T) {
	r := NewResolver(NoopProvider{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = r.Resolve("176.57.218.121")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			r.SwapProvider(NoopProvider{})
		}
	}()
	wg.Wait()
}
