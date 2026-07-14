package geo

import (
	"net"
	"strings"
)

// ParseIP extracts a net.IP from an r.RemoteAddr value. It tolerates every shape the Go
// HTTP stack (with middleware.RealIP) can hand us:
//
//	"1.2.3.4:5678"                  -> 1.2.3.4        (strip :port)
//	"1.2.3.4"                       -> 1.2.3.4
//	"[2606:4700:4700::1111]:443"    -> 2606:4700:...  (bracketed v6 + port)
//	"2606:4700:4700::1111"          -> 2606:4700:...  (bare v6)
//	"fe80::1%eth0"                  -> fe80::1         (strip IPv6 zone)
//	""                              -> nil
//	"garbage" / "999.999.999.999"   -> nil
//
// It never panics; anything unparseable returns nil so the caller treats geo as unknown.
func ParseIP(remoteAddr string) net.IP {
	s := strings.TrimSpace(remoteAddr)
	if s == "" {
		return nil
	}
	// Strip a trailing :port when present. SplitHostPort errors on a bare v4/v6 (no port,
	// or "too many colons"), in which case we keep the original string.
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	// Strip an IPv6 zone identifier ("fe80::1%eth0").
	if i := strings.IndexByte(s, '%'); i >= 0 {
		s = s[:i]
	}
	return net.ParseIP(strings.TrimSpace(s))
}
