package main

import (
	"encoding/json"
	"testing"
)

func TestConsumerResponseShapes(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"sessions":[],"machines":[],"nextCursor":null}`, true},
		{`{"sessions":[],"machines":[],"nextCursor":"next"}`, true},
		{`{"sessions":[],"machines":[]}`, false},
		{`{"sessions":[],"machines":[],"nextCursor":5}`, false},
		{`{"sessions":null,"machines":[],"nextCursor":null}`, false},
	} {
		var result listResult
		err := json.Unmarshal([]byte(tc.body), &result)
		if (err == nil) != tc.valid {
			t.Errorf("list %s: %v", tc.body, err)
		}
	}
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"resources":{"processes":[{"process":{"pid":42},"rssBytes":{"status":"available"}}],"ownedProcessCount":{"status":"unavailable"}}}`, true},
		{`{"resources":{"processes":[],"ownedProcessCount":{"status":"available"}}}`, true},
		{`{"resources":{"processes":[{"process":{},"rssBytes":{"status":"available"}}],"ownedProcessCount":{"status":"available"}}}`, false},
		{`{"resources":{"processes":[{"process":{"pid":42},"rssBytes":{"status":"wrong"}}],"ownedProcessCount":{"status":"available"}}}`, false},
		{`{"resources":{"processes":[{"process":{"pid":42},"rssBytes":{"status":"available"}},{"process":null,"rssBytes":{"status":"available"}}],"ownedProcessCount":{"status":"available"}}}`, false},
		{`{"resources":{"processes":[],"ownedProcessCount":{"status":"wrong"}}}`, false},
		{`{"resources":{"processes":null,"ownedProcessCount":{"status":"available"}}}`, false},
	} {
		var result inspection
		err := json.Unmarshal([]byte(tc.body), &result)
		if (err == nil) != tc.valid {
			t.Errorf("inspect %s: %v", tc.body, err)
		}
	}
}
