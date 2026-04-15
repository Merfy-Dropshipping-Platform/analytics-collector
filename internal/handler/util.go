package handler

import "time"

func timeNow() time.Time {
	return time.Now().UTC()
}
