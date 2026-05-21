package middleware

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

// Logger 请求日志中间件
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()
		clientIP := c.ClientIP()
		method := c.Request.Method

		if raw != "" {
			path = path + "?" + raw
		}

		// 根据状态码设置日志级别
		attrs := []any{
			slog.Int("status", status),
			slog.Duration("latency", latency),
			slog.String("client_ip", clientIP),
			slog.String("method", method),
			slog.String("path", path),
		}

		if status >= 500 {
			if errKind, ok := c.Get("error_kind"); ok {
				attrs = append(attrs, slog.String("error_kind", errKind.(string)))
			}
			if errDetail, ok := c.Get("error_detail"); ok {
				attrs = append(attrs, slog.String("error_detail", errDetail.(string)))
			}
			slog.Error("server error", attrs...)
		} else if status >= 400 {
			slog.Warn("client error", attrs...)
		} else {
			slog.Info("request", attrs...)
		}
	}
}
