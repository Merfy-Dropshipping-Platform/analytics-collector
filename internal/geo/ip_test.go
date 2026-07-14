package geo

import (
	"net"
	"testing"
)

func TestParseIP(t *testing.T) {
	cases := []struct {
		in   string
		want string // "" means expect nil
	}{
		{"1.2.3.4:5678", "1.2.3.4"},
		{"1.2.3.4", "1.2.3.4"},
		{"  1.2.3.4  ", "1.2.3.4"},
		{"[2606:4700:4700::1111]:443", "2606:4700:4700::1111"},
		{"2606:4700:4700::1111", "2606:4700:4700::1111"},
		{"fe80::1%eth0", "fe80::1"},
		{"[fe80::1%eth0]:443", "fe80::1"},
		{"", ""},
		{"garbage", ""},
		{"999.999.999.999", ""},
		{"1.2.3.4.5", ""},
	}
	for _, c := range cases {
		got := ParseIP(c.in)
		if c.want == "" {
			if got != nil {
				t.Errorf("ParseIP(%q) = %v; want nil", c.in, got)
			}
			continue
		}
		want := net.ParseIP(c.want)
		if got == nil || !got.Equal(want) {
			t.Errorf("ParseIP(%q) = %v; want %v", c.in, got, want)
		}
	}
}
