// Command ui serves a local viewer for one ForgeMQ data directory.
package main

import (
	"context"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"forgemq"
)

//go:embed view.html
var page embed.FS

func main() {
	log.SetFlags(0)
	addr := flag.String("addr", "127.0.0.1:8093", "listen address")
	dir := flag.String("dir", "data", "broker data directory")
	flag.Parse()
	abs, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatal(err)
	}
	broker, err := forgemq.Open(abs)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()
	srv := &http.Server{Addr: *addr, Handler: handler(broker, abs)}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("ForgeMQ at http://%s", *addr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case <-ctx.Done():
		_ = srv.Close()
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}

func handler(b *forgemq.Broker, dir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		body, err := page.ReadFile("view.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/api/catalog", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		topics, err := b.Topics()
		if err != nil {
			writeErr(w, err)
			return
		}
		if topics == nil {
			topics = []forgemq.TopicInfo{}
		}
		writeJSON(w, map[string]any{
			"dir":    dir,
			"stats":  b.Stats(),
			"topics": topics,
		})
	})
	mux.HandleFunc("/api/topic", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		name := r.URL.Query().Get("name")
		limit := 40
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				writeErr(w, forgemq.ErrBadRequest)
				return
			}
			limit = n
		}
		_, tail := r.URL.Query()["tail"]
		var from uint64
		if raw := r.URL.Query().Get("from"); raw != "" {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				writeErr(w, forgemq.ErrBadRequest)
				return
			}
			from = n
			tail = false
		} else {
			tail = true
		}
		page, err := b.Inspect(name, from, limit, tail)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, viewPage(page))
	})
	mux.HandleFunc("/api/topics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := decode(r, &body); err != nil {
			writeErr(w, err)
			return
		}
		if err := b.CreateTopic(strings.TrimSpace(body.Name)); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, map[string]any{"name": strings.TrimSpace(body.Name)})
	})
	mux.HandleFunc("/api/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var body struct {
			Topic   string            `json:"topic"`
			Key     string            `json:"key"`
			Payload string            `json:"payload"`
			Headers map[string]string `json:"headers"`
		}
		if err := decode(r, &body); err != nil {
			writeErr(w, err)
			return
		}
		msg := forgemq.Message{
			Topic:   body.Topic,
			Payload: []byte(body.Payload),
			Headers: body.Headers,
		}
		if body.Key != "" {
			msg.Key = []byte(body.Key)
		}
		published, err := b.Publish(msg)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, viewMessage(published))
	})
	return mux
}

func viewPage(page forgemq.Page) map[string]any {
	msgs := make([]map[string]any, len(page.Messages))
	for i, msg := range page.Messages {
		msgs[i] = viewMessage(msg)
	}
	groups := page.Groups
	if groups == nil {
		groups = []forgemq.GroupInfo{}
	}
	return map[string]any{
		"name":     page.Name,
		"base":     page.Base,
		"end":      page.End,
		"from":     page.From,
		"groups":   groups,
		"messages": msgs,
	}
}

func viewMessage(msg forgemq.Message) map[string]any {
	headers := msg.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	payload := ""
	text := true
	if utf8.Valid(msg.Payload) {
		payload = string(msg.Payload)
	} else {
		text = false
		payload = hex.EncodeToString(msg.Payload)
	}
	return map[string]any{
		"id":          msg.ID,
		"offset":      msg.Offset,
		"time":        msg.Timestamp.UTC().Format(time.RFC3339),
		"key":         string(msg.Key),
		"hasKey":      msg.Key != nil,
		"payload":     payload,
		"payloadText": text,
		"headers":     headers,
	}
}

func decode(r *http.Request, dest any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return forgemq.ErrBadRequest
	}
	return nil
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, forgemq.ErrTopicNotFound) {
		status = http.StatusNotFound
	}
	if errors.Is(err, forgemq.ErrClosed) {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", "GET, POST")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}
