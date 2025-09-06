# Golang Consumer Template

A template influenced Clean Architecture featuring in EDA (Event Driven Architecture) systems.

## Getting started

Install virtual env (assume python installed, skip if already installed)

```shell
python -m venv venv
```

Activate virtual env (for Windows)

```shell
./venv/bin/Activate
```

Install pre-commit (skip if already installed)

```shell
pip install pre-commit
pre-commit install
```

Change ```config.yaml``` to an appropriate configuration.

Change ```consumer-blueprint``` to a specific name

Install dependency

```shell
go mod tidy
```

Run without docker (assumed you have Makefile command installed):

```shell
make run
```

## Test

``` shell
make unittest
 ```

## Behind the scenes

Each goroutine will be spawned on demand which means goroutine spawned as soon as a message came. Only number of
goroutines are spawned as much as a specified one in config file. Be careful with a massive high workers will cause the
program crashed. Depending on topics of messages came, the manager will route to an appropriate handler.

***

## Usage

Add more use cases (handlers) in the usecase folder. Each use case represents a topic the use case is responsible for
and must implement Consumer interface with a well-defined model which defined the message format of a topic.
You can learn it by finding an example in the template. You must register a topic for that particular use case
in ``` main.go ``` ( right after you finished writing logics for an use case ). For example:

```go
registra.RegisterTopic(cfg.Kafka.Topic, kafka.NewJsonConsumer(h1))
```

But don't forget to initialize that use case before registering handler.

```go
h1 := usecase.NewUseCase(a.Logger)
```

## Todo

* Consumer of Dead Letter Queue Topic