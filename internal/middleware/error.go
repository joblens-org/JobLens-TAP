package middleware

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
)

// ErrorHandler 统一错误处理中间件
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// 处理验证错误
		if len(c.Errors) > 0 {
			err := c.Errors.Last()
			slog.Error("request error", "error", err.Error())

			c.JSON(http.StatusBadRequest, model.Response{
				Code:    400,
				Message: err.Error(),
			})
		}
	}
}

// Recovery 恢复 panic 中间件
func Recovery() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, err any) {
		slog.Error("panic recovered", "error", err)

		c.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "internal server error",
		})
	})
}
