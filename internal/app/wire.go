//go:build wireinject
// +build wireinject

package app

import "github.com/google/wire"

func initializeApp() (*App, error) {
	wire.Build(
		WithConfig,
		WithLogger,
		WithKafMan,
		WithGru,
		wire.Struct(
			new(App),
			"cfg",
			"logger",
			"kafMan",
			"guru",
		),
	)
	return nil, nil
}
