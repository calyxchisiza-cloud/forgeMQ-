package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"forgemq"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "listen address")
	dir := flag.String("dir", "data", "data directory")
	flag.Parse()
	broker, err := forgemq.Open(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer broker.Close()
	srv, err := forgemq.Listen(*addr, broker)
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
