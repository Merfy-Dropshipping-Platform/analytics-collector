package geo

import "net"

// NoopProvider is the graceful fallback: every lookup returns a zero Location. It is used
// whenever no geo DB is configured or one failed to load, so /collect keeps accepting
// events with geo = null instead of failing.
type NoopProvider struct{}

func (NoopProvider) Lookup(net.IP) Location { return Location{} }
func (NoopProvider) Name() string           { return "noop" }
func (NoopProvider) Close() error           { return nil }
