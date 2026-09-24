package handler

import "time"

// clock — источник «сейчас» для обработчиков. Тесты подменяют его, чтобы границы
// периодов не зависели от момента прогона.
var clock = time.Now

func timeNow() time.Time {
	return clock().UTC()
}
