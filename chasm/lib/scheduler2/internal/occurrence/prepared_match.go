package occurrence

import "time"

func preparedMatches(m CalendarMasks, wall time.Time) bool {
	y, month, day := wall.Date()
	h, minute, sec := wall.Clock()
	if !m.AllYears && (y < 2000 || y > 2100 || m.Years[(y-2000)/64]&(uint64(1)<<((y-2000)%64)) == 0) {
		return false
	}
	values := [6]int{sec, minute, h, day, int(month), int(wall.Weekday())}
	for i, v := range values {
		if m.Fields[i]&(uint64(1)<<v) == 0 {
			return false
		}
	}
	return true
}
