package util

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
)

var priceCleanRe = regexp.MustCompile(`[^\d.,]`)

// ToInt64Price converts flexible price values (string "15 ₽", float 15.0, int 1500) to int64 cents.
func ToInt64Price(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int64(math.Round(val))
	case string:
		cleaned := priceCleanRe.ReplaceAllString(val, "")
		if cleaned == "" {
			return 0
		}
		f, err := strconv.ParseFloat(cleaned, 64)
		if err != nil {
			return 0
		}
		// If value looks like rubles (not cents), convert to cents
		if f < 100000 && f == math.Floor(f) {
			return int64(f) * 100
		}
		return int64(math.Round(f))
	default:
		s := fmt.Sprintf("%v", val)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return int64(math.Round(f))
	}
}
