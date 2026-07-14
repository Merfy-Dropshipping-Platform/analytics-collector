package geo

import (
	"os"
	"strings"
	"testing"
)

// These tests need a real City .mmdb. They are gated on GEO_TEST_MMDB (path to the fixture,
// e.g. scratchpad/geolite2-city.mmdb) and t.Skip otherwise — the 65/131 MB fixtures are NOT
// committed. Run: GEO_TEST_MMDB=/abs/path/geolite2-city.mmdb go test ./internal/geo/...
func mmdbResolver(t *testing.T) *Resolver {
	t.Helper()
	path := os.Getenv("GEO_TEST_MMDB")
	if path == "" {
		t.Skip("GEO_TEST_MMDB not set; skipping mmdb fixture test")
	}
	p, err := OpenMMDB(path)
	if err != nil {
		t.Fatalf("OpenMMDB(%q): %v", path, err)
	}
	t.Cleanup(func() { p.Close() })
	if !strings.HasPrefix(p.Name(), "mmdb:") {
		t.Errorf("provider name %q; want mmdb: prefix", p.Name())
	}
	return NewResolver(p)
}

func TestResolver_AnchorsRU_US(t *testing.T) {
	r := mmdbResolver(t)

	spb := r.Resolve("176.57.218.121")
	if spb.CountryISO != "RU" {
		t.Errorf("176.57.218.121: country=%q; want RU", spb.CountryISO)
	}
	if !strings.Contains(spb.Subject, "Санкт-Петербург") {
		t.Errorf("176.57.218.121: subject=%q; want contains Санкт-Петербург", spb.Subject)
	}

	msk := r.Resolve("92.36.105.198")
	if msk.CountryISO != "RU" {
		t.Errorf("92.36.105.198: country=%q; want RU", msk.CountryISO)
	}
	if !strings.Contains(msk.Subject, "Москва") {
		t.Errorf("92.36.105.198: subject=%q; want contains Москва", msk.Subject)
	}

	us := r.Resolve("8.8.8.8")
	if us.CountryISO != "US" {
		t.Errorf("8.8.8.8: country=%q; want US", us.CountryISO)
	}
	if us.Subject != "" {
		t.Errorf("8.8.8.8: subject=%q; want empty (no subdivision)", us.Subject)
	}
}

func TestResolver_OverrideAnchors_E2E(t *testing.T) {
	r := mmdbResolver(t)
	cases := []struct {
		ip, want string
	}{
		{"2.56.24.1", SubjCrimea},
		{"5.149.208.1", SubjSevastopol},
		{"31.6.126.235", SubjDonetsk},
		{"31.40.132.1", SubjKherson},
		{"45.10.32.1", SubjLuhansk},
		{"2.56.24.217", SubjZaporizhzhia},
	}
	for _, c := range cases {
		loc := r.Resolve(c.ip)
		if loc.CountryISO != "RU" {
			t.Errorf("%s: country=%q; want RU (override)", c.ip, loc.CountryISO)
		}
		if loc.Subject != c.want {
			t.Errorf("%s: subject=%q; want %q", c.ip, loc.Subject, c.want)
		}
	}
}

// The 3 Donbass IPs from the task text resolve to RU with a NOISY subject in GeoLite2
// (Novosibirsk/Rostov). They are the negative case: the override must NOT rewrite them to a
// canonical annexed label (name-stem doesn't match), and they stay RU.
func TestResolver_NoisyDonbassStayRU_NotOverridden(t *testing.T) {
	r := mmdbResolver(t)
	annexed := map[string]bool{
		SubjCrimea: true, SubjSevastopol: true, SubjDonetsk: true,
		SubjLuhansk: true, SubjZaporizhzhia: true, SubjKherson: true,
	}
	for _, ip := range []string{"193.228.160.5", "178.216.232.15", "194.31.152.30"} {
		loc := r.Resolve(ip)
		if loc.CountryISO != "RU" {
			t.Errorf("%s: country=%q; want RU per GeoLite2", ip, loc.CountryISO)
		}
		if annexed[loc.Subject] {
			t.Errorf("%s: subject=%q was wrongly canonicalized (should keep noisy RU label)", ip, loc.Subject)
		}
	}
}

func BenchmarkResolveMMDB(b *testing.B) {
	path := os.Getenv("GEO_TEST_MMDB")
	if path == "" {
		b.Skip("GEO_TEST_MMDB not set")
	}
	p, err := OpenMMDB(path)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	r := NewResolver(p)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Resolve("176.57.218.121")
	}
}
