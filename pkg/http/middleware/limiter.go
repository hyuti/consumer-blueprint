package middleware

import (
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
	"net/http"
)

func LimiterMiddleware(limiter *rate.Limiter) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if limiter.Allow() {
			return
		}
		ctx.AbortWithStatus(http.StatusTooManyRequests)
	}
}
