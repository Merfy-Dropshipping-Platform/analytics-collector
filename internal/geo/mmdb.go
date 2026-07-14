package geo

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	geoip2 "github.com/oschwald/geoip2-golang"
)

// MMDBProvider reads any MaxMind-format .mmdb City database. One reader covers GeoLite2,
// DB-IP City Lite and geoip-base.ru — they share the City schema, only names/coverage differ.
// The reader is opened once and is safe for concurrent Lookup (pure-Go, CGO_ENABLED=0).
type MMDBProvider struct {
	db   *geoip2.Reader
	name string
}

// OpenMMDB reads the .mmdb at path fully into memory (geoip2.FromBytes), NOT geoip2.Open.
// Open mmaps the file and Reader.Close() munmaps it — closing the old reader during a hot-swap
// (StartRefreshLoop) while an in-flight Lookup still reads it is a use-after-free → SIGSEGV.
// FromBytes backs the reader with a Go []byte instead: Close() frees no mapping, and the GC
// keeps the buffer alive as long as any lookup references the old provider. Cost: the DB (~60MB)
// is resident instead of paged — fine for a single City DB, and it makes runtime swap crash-safe.
// The provider name is "mmdb:<basename>" so logs show which base is live.
func OpenMMDB(path string) (*MMDBProvider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mmdb %q: %w", path, err)
	}
	db, err := geoip2.FromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("open mmdb %q: %w", path, err)
	}
	return &MMDBProvider{db: db, name: "mmdb:" + filepath.Base(path)}, nil
}

// Lookup returns coarse geo for ip. Any error or empty record yields a zero Location
// (graceful). The first subdivision is taken as the federal subject and its ISO-3166-2
// code is stashed for the override's primary (name-agnostic) branch.
func (p *MMDBProvider) Lookup(ip net.IP) Location {
	rec, err := p.db.City(ip)
	if err != nil || rec == nil {
		return Location{}
	}
	loc := Location{
		CountryISO: rec.Country.IsoCode,
		City:       pickName(rec.City.Names),
	}
	if len(rec.Subdivisions) > 0 {
		loc.Subject = pickName(rec.Subdivisions[0].Names)
		loc.subjISO = rec.Subdivisions[0].IsoCode
	}
	return loc
}

func (p *MMDBProvider) Name() string { return p.name }

func (p *MMDBProvider) Close() error {
	if p.db == nil {
		return nil
	}
	return p.db.Close()
}

// pickName prefers the Russian localized name, then English, then any non-empty name.
// GeoLite2 ships "ru" for RU places (the reason it was chosen); DB-IP is English-only,
// so this gracefully falls back without special-casing the base.
func pickName(names map[string]string) string {
	if v := names["ru"]; v != "" {
		return v
	}
	if v := names["en"]; v != "" {
		return v
	}
	for _, v := range names {
		if v != "" {
			return v
		}
	}
	return ""
}
