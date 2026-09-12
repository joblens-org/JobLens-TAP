package config

import (
	"testing"
	"time"
)

func TestNormalizeFillsGuardDefaults(t *testing.T) {
	c := &Config{}
	c.Normalize()

	if c.MaxSize != 10000 || c.MaxTimeRangeDays != 7 {
		t.Errorf("basic defaults not filled: %+v", c)
	}
	if c.MaxESBuckets != 65536 || c.MaxMetrics != 20 || c.MaxFlattenFields != 2000 {
		t.Errorf("query guard defaults not filled: %+v", c)
	}
	if c.StreamPageSize != 500 || c.StreamWindowBuckets != 5000 || c.StreamMaxWindows != 2000 {
		t.Errorf("stream defaults not filled: %+v", c)
	}
	if c.QueryTimeout != 60*time.Second || c.StreamTimeout != 10*time.Minute || c.SSEHeartbeat != 15*time.Second {
		t.Errorf("timeout defaults not filled: %+v", c)
	}
}

func TestNormalizeKeepsExplicitValues(t *testing.T) {
	c := &Config{
		MaxSize:              50,
		MaxESBuckets:         1000,
		MaxMetrics:           3,
		MaxFlattenFields:     10,
		StreamPageSize:       10,
		StreamWindowBuckets:  20,
		StreamMaxRecords:     30,
		StreamMaxWindows:     40,
		QueryTimeout:         time.Second,
		StreamTimeout:        2 * time.Second,
		SSEHeartbeat:         time.Minute,
		MaxConcurrentStreams: 2,
	}
	c.Normalize()

	if c.MaxSize != 50 || c.MaxESBuckets != 1000 || c.MaxMetrics != 3 || c.MaxFlattenFields != 10 {
		t.Errorf("explicit query values overwritten: %+v", c)
	}
	if c.StreamPageSize != 10 || c.StreamWindowBuckets != 20 || c.StreamMaxRecords != 30 || c.StreamMaxWindows != 40 {
		t.Errorf("explicit stream values overwritten: %+v", c)
	}
	if c.QueryTimeout != time.Second || c.StreamTimeout != 2*time.Second || c.SSEHeartbeat != time.Minute || c.MaxConcurrentStreams != 2 {
		t.Errorf("explicit timeout values overwritten: %+v", c)
	}
}
