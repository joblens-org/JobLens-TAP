package service

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestResumeCursorRejectsTamperingAndExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	signer := newCursorSigner("test-signing-key")
	cursor := rawResumeCursor{Version: 1, PIT: "pit", Cluster: "c1", Hash: "query", From: now, To: now, After: nil, Expires: now.Add(time.Minute), Done: true}
	token, err := signer.encode(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.decode(token, "query", now); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	parts[0] = base64.RawURLEncoding.EncodeToString([]byte(strings.ReplaceAll(string(data), "c1", "c2")))
	if _, err := signer.decode(strings.Join(parts, "."), "query", now); err == nil {
		t.Fatal("接受了篡改游标")
	}
	if _, err := signer.decode(token, "other", now); err == nil {
		t.Fatal("接受了其他查询的游标")
	}
	if _, err := signer.decode(token, "query", now.Add(2*time.Minute)); err == nil {
		t.Fatal("接受了过期游标")
	}
}
