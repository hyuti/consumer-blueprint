run:
	air -c ./cmd/consumer/.air.toml

unittest:
	go test ./... -v

build:
	docker build . --tag consumer-golang-template

containerrun:
	docker run consumer-golang-template
