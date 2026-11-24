.PHONY: test race demo serve

test:
	go test ./...

race:
	go test -race ./...

demo:
	go run ./cmd/demo

serve:
	go run ./cmd/forgemq
