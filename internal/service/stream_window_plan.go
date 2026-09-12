package service

import (
	"math"
	"time"
)

type timeWindow struct {
	from      time.Time
	to        time.Time
	inclusive bool
}

func planTimeWindows(from, to time.Time, interval time.Duration, buckets int64, limit int) ([]timeWindow, error) {
	if interval < time.Millisecond || buckets < 1 || buckets > math.MaxInt64/int64(interval) || limit < 1 || to.Before(from) {
		return nil, NewQueryError(StatusBadRequest, ErrKindInvalidInterval, "invalid window interval or budget")
	}
	millis := interval.Milliseconds()
	originMS := from.UnixMilli() - ((from.UnixMilli()%millis + millis) % millis)
	origin := time.UnixMilli(originMS).UTC()
	span := interval * time.Duration(buckets)
	count := int64(to.Sub(origin)/span) + 1
	if count > int64(limit) {
		return nil, NewQueryError(StatusBadRequest, ErrKindTooManyWindows, "%d windows exceeds limit %d", count, limit)
	}
	windows := make([]timeWindow, 0, int(count))
	start := from
	for i := int64(0); i < count; i++ {
		last := i == count-1
		end := origin.Add(time.Duration(i+1) * span)
		if last {
			end = to
		}
		windows = append(windows, timeWindow{from: start, to: end, inclusive: last})
		start = end
	}
	return windows, nil
}
