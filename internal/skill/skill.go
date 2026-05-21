// Package skill 管理 OpenCode skill 文档的嵌入和渲染
package skill

import (
	_ "embed"
	"strings"
)

//go:embed api_skill.md
var apiSkillTemplate string

// RenderAPISkill 渲染 joblens-tap-api skill 文档，将 [BASE_URL] 替换为实际 API 地址
func RenderAPISkill(baseURL string) string {
	return strings.ReplaceAll(apiSkillTemplate, "[BASE_URL]", baseURL)
}
