package model

import (
	"github.com/hyuti/Consumer-Golang-Template/pkg/tool"
)

type (
	Model struct {
		Field1 string `json:"field_1"`
	}
)

func (d Model) String() string {
	return tool.JSONStringify(&d)
}
func (Model) Name() string {
	return "Model"
}
