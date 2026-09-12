package service

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/joblens/tap/internal/config"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/repository"
)

type rawResumeCursor struct {
	Done    bool              `json:"done,omitempty"`
	Version int               `json:"v"`
	PIT     string            `json:"pit"`
	Cluster string            `json:"cluster"`
	From    time.Time         `json:"from"`
	To      time.Time         `json:"to"`
	After   []json.RawMessage `json:"after"`
	Hash    string            `json:"hash"`
	Expires time.Time         `json:"expires"`
}

func rawStreamHash(req *model.RawStreamRequest) string {
	copy := *req
	copy.Cursor, copy.Format = "", ""
	copy.MaxRecords, copy.PageSize = 0, 0
	data, err := json.Marshal(copy)
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

type cursorSigner struct{ key []byte }

func newCursorSigner(key string) cursorSigner {
	if key == "" {
		key = rand.Text()
	}
	return cursorSigner{key: []byte(key)}
}

func (s cursorSigner) encode(cursor rawResumeCursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s cursorSigner) decode(token, hash string, now time.Time) (*rawResumeCursor, error) {
	if len(token) > 64<<10 {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "cursor too large")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "invalid cursor")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "invalid cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "invalid cursor signature")
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(data)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "invalid cursor signature")
	}
	var cursor rawResumeCursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.Version != 1 || cursor.PIT == "" || (!cursor.Done && len(cursor.After) != 2) || cursor.Hash != hash || cursor.Cluster == "" || cursor.To.Before(cursor.From) {
		return nil, NewQueryError(400, ErrKindInvalidRequest, "cursor does not match query")
	}
	if !now.Before(cursor.Expires) {
		return nil, NewQueryError(410, "cursor_expired", "snapshot cursor expired; restart query")
	}
	return &cursor, nil
}

func (it *rawStreamIter) resumeToken(done bool) (string, error) {
	after := make([]json.RawMessage, len(it.committedAfter))
	for i, v := range it.committedAfter {
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		after[i] = data
	}
	cursor := rawResumeCursor{Version: 1, PIT: it.pitID, Cluster: it.id, From: it.plan.from, To: it.plan.to, After: after, Hash: it.hash, Expires: it.leaseUntil, Done: done}
	return it.signer.encode(cursor)
}

func (s *StreamService) resumeRawPlan(id string, req *model.RawQueryRequest, resume *rawResumeCursor) (*rawQueryPlan, *repository.ESClient, error) {
	name, tag := config.ParseClusterFilter(id)
	if tag == "" {
		_, tag = config.ParseClusterFilter(req.Cluster)
	}
	client, info, err := s.q.esManager.GetClientForCluster(name)
	if err != nil {
		return nil, nil, err
	}
	if tag == "" && len(info.Tags) == 1 {
		tag = info.Tags[0]
	}
	filters, err := s.q.parserSvc.BuildJobFilter(req.Job)
	if err != nil {
		return nil, nil, err
	}
	return &rawQueryPlan{clusterName: info.Name, clusterTag: tag, routing: tag, fields: s.q.parserSvc.ParseFields(req.Fields), from: resume.From, to: resume.To, jobFilters: filters}, client, nil
}
