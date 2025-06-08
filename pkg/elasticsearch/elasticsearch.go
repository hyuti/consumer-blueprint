package elasticsearch

import (
	els "github.com/elastic/go-elasticsearch/v8"
)

type Cfg struct {
	Username  string
	Password  string
	Addresses []string
}

func New(
	config *Cfg,
) (*els.TypedClient, error) {
	return els.NewTypedClient(els.Config{
		Addresses:     config.Addresses,
		Username:      config.Username,
		Password:      config.Password,
		RetryOnStatus: []int{502, 503, 504, 429},
	})
}
