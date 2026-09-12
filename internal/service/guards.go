package service

import "time"

const groupByTermsSize = 100

func dateBucketCount(from, to time.Time, interval time.Duration) int64 {
	if interval <= 0 || !to.After(from) {
		return 0
	}
	return int64(to.Sub(from)/interval) + 1
}

func estimateBuckets(dateBuckets int64, metricCount int, grouped bool) int64 {
	groupFactor := int64(1)
	if grouped {
		groupFactor = groupByTermsSize
	}
	return dateBuckets * groupFactor * int64(1+metricCount)
}

func windowDateBuckets(maxESBuckets, windowBuckets, metricCount int, grouped bool) int64 {
	groupFactor := 1
	if grouped {
		groupFactor = groupByTermsSize
	}
	hard := int64(maxESBuckets) / (int64(groupFactor) * int64(1+metricCount))
	if hard < 1 {
		hard = 1
	}
	limit := int64(windowBuckets)
	if hard < limit {
		limit = hard
	}
	return limit
}
