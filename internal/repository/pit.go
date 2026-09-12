package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type SearchError struct {
	Status int
	Kind   string
}

func (e *SearchError) Error() string {
	return fmt.Sprintf("elasticsearch %s (status %d)", e.Kind, e.Status)
}

func (c *ESClient) OpenPIT(ctx context.Context, indices []string, routing string, ttl time.Duration) (string, error) {
	opts := []func(*esapi.OpenPointInTimeRequest){
		c.client.OpenPointInTime.WithContext(ctx),
		c.client.OpenPointInTime.WithAllowPartialSearchResults(false),
	}
	if routing != "" {
		opts = append(opts, c.client.OpenPointInTime.WithRouting(routing))
	}
	resp, err := c.client.OpenPointInTime(indices, strconv.FormatInt(ttl.Milliseconds(), 10)+"ms", opts...)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return "", &SearchError{Status: resp.StatusCode, Kind: "open_pit_failed"}
	}
	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", &SearchError{Status: 502, Kind: "missing_pit_id"}
	}
	return result.ID, nil
}

func (c *ESClient) ClosePIT(ctx context.Context, id string) error {
	body, err := json.Marshal(struct {
		ID string `json:"id"`
	}{id})
	if err != nil {
		return err
	}
	resp, err := c.client.ClosePointInTime(c.client.ClosePointInTime.WithContext(ctx), c.client.ClosePointInTime.WithBody(bytes.NewReader(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return &SearchError{Status: resp.StatusCode, Kind: "close_pit_failed"}
	}
	return nil
}

func (c *ESClient) SearchPIT(ctx context.Context, query map[string]any, maxBytes int64) (*SearchResult, error) {
	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Search(c.client.Search.WithContext(ctx), c.client.Search.WithBody(bytes.NewReader(body)), c.client.Search.WithAllowPartialSearchResults(false))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return nil, &SearchError{Status: resp.StatusCode, Kind: "search_failed"}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, &SearchError{Status: 413, Kind: "response_too_large"}
	}
	var wire struct {
		PITID    string `json:"pit_id"`
		TimedOut bool   `json:"timed_out"`
		Shards   struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits struct {
			Hits []struct {
				ID     string            `json:"_id"`
				Index  string            `json:"_index"`
				Sort   []json.RawMessage `json:"sort"`
				Source map[string]any    `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	if wire.TimedOut {
		return nil, &SearchError{Status: 504, Kind: "query_timeout"}
	}
	if wire.Shards.Failed > 0 {
		return nil, &SearchError{Status: 502, Kind: "partial_search"}
	}
	result := &SearchResult{PITID: wire.PITID}
	for _, h := range wire.Hits.Hits {
		sort := make([]any, len(h.Sort))
		for i, v := range h.Sort {
			sort[i] = v
		}
		result.Hits = append(result.Hits, SearchHit{ID: h.ID, Index: h.Index, Source: h.Source, Sort: sort})
	}
	return result, nil
}
