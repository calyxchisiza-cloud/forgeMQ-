package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"forgemq"
)

func TestViewerListsTopicsAndAppendsWithoutAGroup(t *testing.T) {
	dir := t.TempDir()
	broker, err := forgemq.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { broker.Close() })
	if err := broker.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish(forgemq.Message{Topic: "orders.created", Payload: []byte(`{"order_id":1}`)}); err != nil {
		t.Fatal(err)
	}
	accounting, err := broker.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler(broker, dir))
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/api/catalog")
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Dir    string              `json:"dir"`
		Topics []forgemq.TopicInfo `json:"topics"`
	}
	if err := json.NewDecoder(res.Body).Decode(&catalog); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if catalog.Dir != dir || len(catalog.Topics) != 1 || catalog.Topics[0].Name != "orders.created" {
		t.Fatalf("catalog %+v", catalog)
	}
	if len(catalog.Topics[0].Groups) != 1 || catalog.Topics[0].Groups[0].Commit != 0 {
		t.Fatalf("groups %+v", catalog.Topics[0].Groups)
	}

	res, err = http.Get(srv.URL + "/api/topic?name=orders.created")
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Messages []struct {
			Offset  uint64 `json:"offset"`
			Payload string `json:"payload"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(res.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if len(page.Messages) != 1 || page.Messages[0].Payload != `{"order_id":1}` {
		t.Fatalf("page %+v", page)
	}
	if accounting.Offset() != 0 {
		t.Fatalf("offset moved to %d", accounting.Offset())
	}

	body, _ := json.Marshal(map[string]string{"name": "payments.completed"})
	res, err = http.Post(srv.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create %s", res.Status)
	}
	res.Body.Close()

	body, _ = json.Marshal(map[string]any{
		"topic":   "orders.created",
		"key":     "order:2",
		"payload": "two",
		"headers": map[string]string{"kind": "created"},
	})
	res, err = http.Post(srv.URL+"/api/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("publish %s", res.Status)
	}
	res.Body.Close()
	if accounting.Offset() != 0 {
		t.Fatalf("publish moved the group to %d", accounting.Offset())
	}

	pageRes, err := http.Get(srv.URL + "/api/topic?name=orders.created&from=1")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(pageRes.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	pageRes.Body.Close()
	if len(page.Messages) != 1 || page.Messages[0].Offset != 1 || page.Messages[0].Payload != "two" {
		t.Fatalf("appended %+v", page.Messages)
	}

	home, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if home.StatusCode != http.StatusOK || home.Header.Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("home %s %s", home.Status, home.Header.Get("Content-Type"))
	}
	home.Body.Close()
	if filepath.Base(dir) == "" {
		t.Fatal("dir")
	}
}
