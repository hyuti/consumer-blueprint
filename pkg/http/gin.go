package http

import (
	"encoding/json"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	ctx2 "github.com/hyuti/API-Golang-Template/pkg/ctx"
	"golang.org/x/exp/slog"
	"net/http"
	"time"

	"context"
)

type Cfg struct {
	Debug  bool
	Logger *slog.Logger
	Name   string
}

func New(cfg *Cfg) *gin.Engine {
	gin.SetMode(gin.DebugMode)
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	server := gin.New()

	server.Use(
		gin.LoggerWithConfig(gin.LoggerConfig{
			Formatter: func(param gin.LogFormatterParams) string {
				r := struct {
					IP        string        `json:"ip_addr"`
					Time      string        `json:"time"`
					Method    string        `json:"method"`
					Path      string        `json:"path"`
					Proto     string        `json:"proto"`
					StsCode   int           `json:"status_code"`
					Latency   time.Duration `json:"latency"`
					UserAgent string        `json:"user_agent"`
					ErrMsg    string        `json:"err_msg"`
					CtxID     any           `json:"x-context-id"`
				}{
					IP:        param.ClientIP,
					Time:      param.TimeStamp.Format(time.RFC3339Nano),
					Method:    param.Method,
					Path:      param.Path,
					Proto:     param.Request.Proto,
					StsCode:   param.StatusCode,
					Latency:   param.Latency,
					UserAgent: param.Request.UserAgent(),
					ErrMsg:    param.ErrorMessage,
					CtxID:     param.Keys[ctx2.CtxIDKey],
				}
				b, err := json.Marshal(&r)
				if err != nil {
					return ""
				}
				return string(b)
			},
			Output: &logger{
				Logger: cfg.Logger,
			},
		}),
	)
	server.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodDelete,
			http.MethodPut,
			http.MethodPatch,
			http.MethodOptions,
		},
		AllowHeaders:           []string{"Origin", "Authorization", "Content-Type", "token"},
		AllowBrowserExtensions: true,
		AllowWebSockets:        true,
		AllowFiles:             true,
	}))
	server.Use(func(c *gin.Context) {
		if ctx2.GetCtxID(c.Request.Context()) != "" {
			return
		}
		v := c.Value(ctx2.CtxIDKey)
		if v == nil {
			return
		}
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctx2.CtxIDKey, v))
	})
	return server
}
