package grpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
)

type SrvConfig struct {
	Port string
}

type Server struct {
	srv *grpc.Server
	lis net.Listener
	opt []grpc.ServerOption
}

func New(cfg *SrvConfig) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", cfg.Port))
	if err != nil {
		return nil, err
	}
	srv := Server{
		lis: lis,
	}
	return &srv, nil
}
func (s *Server) WithOpt(opt ...grpc.ServerOption) {
	s.opt = append(s.opt, opt...)
}

func (s *Server) Run() error {
	s.Init()
	return s.srv.Serve(s.lis)
}

func (s *Server) Init() {
	if s.srv != nil {
		return
	}
	s.srv = grpc.NewServer(s.opt...)
}

func (s *Server) Server() *grpc.Server {
	s.Init()
	return s.srv
}
