package geo

import "strings"

// Canonical labels for the annexed territories (authority = SPEC §2). Exported so tests
// and downstream code reference the constant, never a hardcoded string.
const (
	SubjCrimea       = "Республика Крым"
	SubjSevastopol   = "Севастополь"
	SubjDonetsk      = "ДНР (Донецкая обл.)"
	SubjLuhansk      = "ЛНР (Луганская обл.)"
	SubjZaporizhzhia = "Запорожская обл."
	SubjKherson      = "Херсонская обл."
)

// annexStems maps a case-insensitive substring stem (Latin + Cyrillic) to the canonical
// subject. Substring (not exact equality) is deliberate: a live scan showed the bases spell
// these many ways — "Sebastopol City", "Kherson Oblast", "Zaporizhzhya Oblast",
// "Gorod Sevastopol", "Донецкая область" — which exact-match would miss but a stem catches.
var annexStems = []struct{ stem, subject string }{
	{"crimea", SubjCrimea},
	{"krym", SubjCrimea}, // транслит "Krym"/"Respublika Krym" (DB-IP/иные базы)
	{"крым", SubjCrimea},
	{"sevastopol", SubjSevastopol},
	{"sebastopol", SubjSevastopol},
	{"севастопол", SubjSevastopol},
	{"donetsk", SubjDonetsk},
	{"донецк", SubjDonetsk},
	{"luhansk", SubjLuhansk},
	{"lugansk", SubjLuhansk},
	{"луганск", SubjLuhansk},
	{"zaporizh", SubjZaporizhzhia},
	{"zaporozh", SubjZaporizhzhia},
	{"запорож", SubjZaporizhzhia},
	{"kherson", SubjKherson},
	{"херсон", SubjKherson},
}

// annexISO maps a UA ISO-3166-2 subdivision code (without the "UA-" prefix) to the canonical
// subject. This is the primary, name-agnostic override signal (robust to noisy region names).
// RU subdivisions in mmdb use alphabetic codes (MOW/SPE/ROS), so these numeric UA codes never
// collide — hence the ISO branch is gated on CountryISO == "UA".
var annexISO = map[string]string{
	"43": SubjCrimea,
	"40": SubjSevastopol,
	"14": SubjDonetsk,
	"09": SubjLuhansk,
	"23": SubjZaporizhzhia,
	"65": SubjKherson,
}

// applyOverride forces country=RU + a canonical Russian subject label for the annexed
// territories, working over ANY base (UA in GeoLite2, sometimes RU in DB-IP) and normalizing
// the label to one form across bases.
//
// Two branches, in order:
//  1. UA subdivision ISO code — most reliable; unaffected by how the base spells the name.
//  2. Subject-name stem — case-insensitive substring; also catches RU-labeled "Crimea"/
//     "Sevastopol" from DB-IP so the label is unified regardless of source country.
//
// Everything else is returned unchanged: mainland UA (Kyiv/Lviv) and RU regions that the
// base happens to place noisily on annexed IPs (e.g. Rostov/Novosibirsk) are NOT touched.
func applyOverride(loc Location) Location {
	if loc.CountryISO == "UA" && loc.subjISO != "" {
		if s, ok := annexISO[loc.subjISO]; ok {
			loc.CountryISO, loc.Subject = "RU", s
			return loc
		}
	}
	if loc.Subject != "" {
		low := strings.ToLower(loc.Subject)
		for _, a := range annexStems {
			if strings.Contains(low, a.stem) {
				loc.CountryISO, loc.Subject = "RU", a.subject
				return loc
			}
		}
	}
	return loc
}
