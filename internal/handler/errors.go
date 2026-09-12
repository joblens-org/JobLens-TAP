package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/service"
)

func respondQueryError(c *gin.Context, err error) bool {
	var qe *service.QueryError
	if !errors.As(err, &qe) {
		return false
	}
	c.Set("error_kind", qe.Kind)
	c.Set("error_detail", qe.Msg)
	c.JSON(qe.Status, model.Response{
		Code:    qe.Status,
		Message: qe.Msg,
	})
	return true
}

func respondBadRequest(c *gin.Context, kind, message string) {
	c.Set("error_kind", kind)
	c.Set("error_detail", message)
	c.JSON(http.StatusBadRequest, model.Response{
		Code:    http.StatusBadRequest,
		Message: message,
	})
}
