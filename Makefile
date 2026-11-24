.PHONY: test race bench demo serve ui

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench=. -benchmem -count=1 -run=^$$ .

demo:
	go run ./cmd/demo

serve:
	go run ./cmd/forgemq

ui:
	go run ./cmd/ui
