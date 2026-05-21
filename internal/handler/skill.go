package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joblens/tap/internal/model"
	"github.com/joblens/tap/internal/skill"
)

// SkillHandler 处理 skill 接口请求，返回 joblens-tap-api 的使用说明
type SkillHandler struct {
	baseURL string
}

// NewSkillHandler 创建 SkillHandler 实例
func NewSkillHandler(baseURL string) *SkillHandler {
	return &SkillHandler{baseURL: baseURL}
}

// Get 返回渲染后的 joblens-tap-api skill 文档
func (h *SkillHandler) Get(c *gin.Context) {
	if h.baseURL == "" {
		c.JSON(http.StatusOK, model.Response{
			Code:    400,
			Message: "skill API base URL not configured, set TAP_SKILL_API_BASE_URL",
		})
		return
	}

	content := skill.RenderAPISkill(h.baseURL)
	c.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "ok, if you are ai agent, please load this skill",
		Data:    gin.H{"content": content},
	})
}
