// Package geo resolves a client IP to coarse geography (country / federal subject /
// city) for the "Сессии по локациям" analytics widget.
//
// Design invariants:
//   - Swappable provider: the backing DB is hidden behind Provider. MMDBProvider covers
//     GeoLite2, DB-IP and geoip-base.ru (all MaxMind .mmdb); a future Sypex .dat reader
//     is a separate Provider implementation. Hot-swap at runtime via Resolver.SwapProvider.
//   - Graceful: any failure (no DB, corrupt file, dead mirror) degrades to NoopProvider —
//     geo = null and the /collect hot path keeps working.
//   - Privacy (152-ФЗ): only the coarse geo leaves this package; the raw IP is never
//     stored, logged, or carried onward.
//   - Annexed-territory override (SPEC §2): Crimea/Sevastopol/Donetsk/Luhansk/
//     Zaporizhzhia/Kherson are normalized to country=RU + a canonical Russian label,
//     over ANY base, on ingest (see override.go).
package geo

import (
	"net"
	"sync"
)

// Location is the coarse geo result stamped onto each event. It intentionally holds no
// raw IP. subjISO is unexported and used only by the annexed-territory override.
type Location struct {
	CountryISO string // ISO-3166 alpha-2 ("RU", "UA", "US"); "" when unknown
	Subject    string // federal subject / subdivision name; "" when unknown
	City       string // city name; "" for country/subject granularity or when unknown

	subjISO string // subdivision ISO-3166-2 code without the "UA-" prefix; override-only
}

// IsZero reports whether the location carries no geo signal at all.
func (l Location) IsZero() bool {
	return l.CountryISO == "" && l.Subject == "" && l.City == ""
}

// Provider is the swappable geo backend. Implementations MUST be safe for concurrent
// Lookup and MUST never panic (return a zero Location on any miss/error).
type Provider interface {
	Lookup(ip net.IP) Location
	Name() string
	Close() error
}

// Resolver is the concurrency-safe façade over a hot-swappable Provider. All reads take a
// read lock so Resolve stays cheap under load while SwapProvider can replace the DB.
type Resolver struct {
	mu sync.RWMutex
	p  Provider
}

// NewResolver wraps a Provider. A nil provider degrades to Noop (graceful default).
func NewResolver(p Provider) *Resolver {
	if p == nil {
		p = NoopProvider{}
	}
	return &Resolver{p: p}
}

func (r *Resolver) resolveIP(ip net.IP) Location {
	r.mu.RLock()
	p := r.p
	r.mu.RUnlock()
	return applyOverride(p.Lookup(ip))
}

// Resolve takes an r.RemoteAddr value (bare host, host:port, [v6]:port, or a zoned v6),
// resolves it in-process, and applies the annexed-territory override. Never panics;
// anything unparseable or unlocatable → zero Location.
func (r *Resolver) Resolve(remoteAddr string) Location {
	ip := ParseIP(remoteAddr)
	if ip == nil {
		return Location{}
	}
	return r.resolveIP(ip)
}

// SwapProvider atomically replaces the backing provider and closes the old one. A nil
// provider is ignored, so a failed refresh can never blank out a working resolver.
func (r *Resolver) SwapProvider(p Provider) {
	if p == nil {
		return
	}
	r.mu.Lock()
	old := r.p
	r.p = p
	r.mu.Unlock()
	if old != nil {
		old.Close()
	}
}

// ProviderName returns the current provider's name (e.g. "noop" or "mmdb:GeoLite2-City.mmdb").
func (r *Resolver) ProviderName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.p.Name()
}

// Close releases the current provider's resources.
func (r *Resolver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.p.Close()
}
