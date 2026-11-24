.PHONY: test race demo

test:
	go test ./...

race:
	go test -race ./...

demo:
	go run ./cmd/demo
