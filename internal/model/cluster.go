package model

// ClusterMeta 集群元数据（来源于管理 API）
type ClusterMeta struct {
	Name    string         `json:"cluster_name"` // 集群名称
	Type    string         `json:"cluster_type"` // 集群类型: condor / slurm
	Tags    []string       `json:"tags"`         // 集群标签列表（ES routing 键）
	Alias   string         `json:"alias"`        // 显示别名
	Enabled bool           `json:"enabled"`      // 是否启用
	Extra   map[string]any `json:"extra"`        // 扩展字段（es_url/es_username/es_password/script_path/default_node_port 等）

	// 从 Extra 拉平的便捷字段
	ESURL           string
	ESUsername      string
	ESPassword      string
	ScriptPath      string
	DefaultNodePort int
}

// ManagementAPIResponse GET /api/clusters/scheme 接口响应
type ManagementAPIResponse struct {
	Clusters []ClusterMeta `json:"clusters"`
	Total    int           `json:"total"`
}

// NormalizeExtra 从 Extra 提取便捷字段
func (c *ClusterMeta) NormalizeExtra() {
	if c.Extra == nil {
		return
	}
	if v, ok := c.Extra["es_url"].(string); ok {
		c.ESURL = v
	}
	if v, ok := c.Extra["es_username"].(string); ok {
		c.ESUsername = v
	}
	if v, ok := c.Extra["es_password"].(string); ok {
		c.ESPassword = v
	}
	if v, ok := c.Extra["script_path"].(string); ok {
		c.ScriptPath = v
	}
	if v, ok := c.Extra["default_node_port"]; ok {
		switch val := v.(type) {
		case float64:
			c.DefaultNodePort = int(val)
		case int:
			c.DefaultNodePort = val
		case int64:
			c.DefaultNodePort = int(val)
		}
	}
	if c.DefaultNodePort == 0 {
		c.DefaultNodePort = 8080
	}
}
