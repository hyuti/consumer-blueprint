run:
	air -c ./.air.toml

unittest:
	go test ./... -v

build:
	docker build . --tag consumer-blueprint

containerrun:
	docker run consumer-blueprint
