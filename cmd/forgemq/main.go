package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"forgemq"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "listen address")
	dir := flag.String("dir", "data", "data directory")
	token := flag.String("token", "", "client token; empty allows every connection")
	cert := flag.String("cert", "", "TLS certificate file")
	key := flag.String("key", "", "TLS private key file")
	flag.Parse()
	if (*cert == "") != (*key == "") {
		fmt.Fprintln(os.Stderr, "cert and key are set together")
		os.Exit(1)
	}
	cfg := forgemq.Config{
		Dir:    *dir,
		Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	if *token != "" {
		cfg.Principals = []forgemq.Principal{{
			Token:   *token,
			Publish: true,
			Consume: true,
		}}
	}
	broker, err := forgemq.OpenWith(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer broker.Close()
	var srv *forgemq.Server
	if *cert != "" {
		pair, loadErr := tls.LoadX509KeyPair(*cert, *key)
		if loadErr != nil {
			fmt.Fprintln(os.Stderr, loadErr)
			os.Exit(1)
		}
		srv, err = forgemq.ListenTLS(*addr, broker, &tls.Config{
			Certificates: []tls.Certificate{pair},
			MinVersion:   tls.VersionTLS12,
		})
	} else {
		srv, err = forgemq.Listen(*addr, broker)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer srv.Close()
	fmt.Printf("ForgeMQ listening on %s\n", srv.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
}
