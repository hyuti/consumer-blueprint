package grpc

import (
	protovalidate "github.com/bufbuild/protovalidate-go"
)

var validator *protovalidate.Validator

func Validator() *protovalidate.Validator {
	return validator
}

func DefaultValidator(options ...protovalidate.ValidatorOption) (*protovalidate.Validator, error) {
	_validator, err := protovalidate.New(options...)
	if err != nil {
		return nil, err
	}
	return NewValidator(_validator), nil
}

func NewValidator(_validator *protovalidate.Validator) *protovalidate.Validator {
	validator = _validator
	return validator
}
