package geo

import "testing"

// applyOverride is exercised directly (fixture-free) — no mmdb needed. It is the load-bearing
// invariant: annexed territories always normalize to RU + a canonical label; everything else
// is left untouched.

func TestApplyOverride_SixCanonPositive(t *testing.T) {
	cases := []struct {
		country, subject, want string
	}{
		{"UA", "Crimea", SubjCrimea},
		{"UA", "Donetsk", SubjDonetsk},
		{"UA", "Luhansk", SubjLuhansk},
		{"UA", "Zaporizhzhia", SubjZaporizhzhia},
		{"UA", "Kherson", SubjKherson},
		{"UA", "Sevastopol", SubjSevastopol},
	}
	for _, c := range cases {
		got := applyOverride(Location{CountryISO: c.country, Subject: c.subject})
		if got.CountryISO != "RU" || got.Subject != c.want {
			t.Errorf("applyOverride(%q,%q) = (%q,%q); want (RU,%q)",
				c.country, c.subject, got.CountryISO, got.Subject, c.want)
		}
	}
}

func TestApplyOverride_SpellingVariants_StemMatch(t *testing.T) {
	// Exact equality would miss all of these; the substring stem catches them.
	cases := []struct {
		subject, want string
	}{
		{"Sebastopol City", SubjSevastopol},
		{"Kherson Oblast", SubjKherson},
		{"Zaporizhzhya Oblast", SubjZaporizhzhia},
		{"Gorod Sevastopol", SubjSevastopol},
		{"Lugansk", SubjLuhansk},
		{"Zaporozhye", SubjZaporizhzhia},
		{"Donetsk People's Republic", SubjDonetsk},
	}
	for _, c := range cases {
		got := applyOverride(Location{CountryISO: "UA", Subject: c.subject})
		if got.CountryISO != "RU" || got.Subject != c.want {
			t.Errorf("applyOverride(UA,%q) = (%q,%q); want (RU,%q)",
				c.subject, got.CountryISO, got.Subject, c.want)
		}
	}
}

func TestApplyOverride_CaseInsensitiveAndCyrillic(t *testing.T) {
	cases := []struct {
		country, subject, want string
	}{
		{"ua", "crimea", SubjCrimea},
		{"UA", "CRIMEA", SubjCrimea},
		{"UA", "Крым", SubjCrimea},
		{"UA", "Республика Крым", SubjCrimea},
		{"UA", "Севастополь", SubjSevastopol},
		{"UA", "Донецкая область", SubjDonetsk},
		{"UA", "Луганская область", SubjLuhansk},
		{"UA", "Запорожская область", SubjZaporizhzhia},
		{"UA", "Херсонская область", SubjKherson},
	}
	for _, c := range cases {
		got := applyOverride(Location{CountryISO: c.country, Subject: c.subject})
		if got.CountryISO != "RU" || got.Subject != c.want {
			t.Errorf("applyOverride(%q,%q) = (%q,%q); want (RU,%q)",
				c.country, c.subject, got.CountryISO, got.Subject, c.want)
		}
	}
}

func TestApplyOverride_RULabeledNormalized(t *testing.T) {
	// DB-IP already returns RU for Crimea/Sevastopol; override still fires so the label is
	// identical across bases (base-agnostic downstream storage).
	for _, c := range []struct{ subject, want string }{
		{"Crimea", SubjCrimea},
		{"Sevastopol", SubjSevastopol},
	} {
		got := applyOverride(Location{CountryISO: "RU", Subject: c.subject})
		if got.CountryISO != "RU" || got.Subject != c.want {
			t.Errorf("applyOverride(RU,%q) = (%q,%q); want (RU,%q)",
				c.subject, got.CountryISO, got.Subject, c.want)
		}
	}
}

func TestApplyOverride_ISOCodeBranch(t *testing.T) {
	// Primary branch: UA subdivision ISO code, robust to a noisy/absent name.
	cases := []struct {
		iso, want string
	}{
		{"43", SubjCrimea},
		{"40", SubjSevastopol},
		{"14", SubjDonetsk},
		{"09", SubjLuhansk},
		{"23", SubjZaporizhzhia},
		{"65", SubjKherson},
	}
	for _, c := range cases {
		got := applyOverride(Location{CountryISO: "UA", subjISO: c.iso})
		if got.CountryISO != "RU" || got.Subject != c.want {
			t.Errorf("applyOverride(UA, subjISO=%q) = (%q,%q); want (RU,%q)",
				c.iso, got.CountryISO, got.Subject, c.want)
		}
	}
}

func TestApplyOverride_Negatives_Untouched(t *testing.T) {
	// Must NOT fire. "Rostov"/"Novosibirsk" are exactly the noisy RU subjects GeoLite2 puts
	// on the 3 Donbass IPs from the task — override must leave them alone.
	cases := []Location{
		{CountryISO: "UA", Subject: "Kyiv"},
		{CountryISO: "UA", Subject: "Kyiv City"},
		{CountryISO: "UA", Subject: "Lviv"},
		{CountryISO: "UA", Subject: "Odesa"},
		{CountryISO: "RU", Subject: "Rostov Oblast"},
		{CountryISO: "RU", Subject: "Novosibirsk Oblast"},
		{CountryISO: "US", Subject: "California"},
		{CountryISO: "", Subject: ""},
	}
	for _, in := range cases {
		got := applyOverride(in)
		if got.CountryISO != in.CountryISO || got.Subject != in.Subject {
			t.Errorf("applyOverride(%q,%q) changed to (%q,%q); want unchanged",
				in.CountryISO, in.Subject, got.CountryISO, got.Subject)
		}
	}
}

// ISO branch is gated on UA: a numeric code under a non-UA country must not trigger.
func TestApplyOverride_ISOGatedOnUA(t *testing.T) {
	got := applyOverride(Location{CountryISO: "RU", subjISO: "43", Subject: "Moscow"})
	if got.CountryISO != "RU" || got.Subject != "Moscow" {
		t.Errorf("ISO branch fired under non-UA country: got (%q,%q)", got.CountryISO, got.Subject)
	}
}
