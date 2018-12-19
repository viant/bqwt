package bqwt

import "time"

//TimeWindow represents a table time winfow
type TimeWindow struct {
	From time.Time
	To   time.Time
}
