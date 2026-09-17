# JobLens-TAP 性能优化清单

> 版本：基于 `e550203`（含流式性能埋点与 pprof）实测与代码走查。
> 更新：2026-09-18 已在 83.20 实现 P0-1、P0-2 并完成 ABBA 对比实测（见各条目"实测"小节）。
> 数据来源：2026-09-17 JobLens visualization 详情流压测（82.26 → 本服务 → omat4htc-es），
> 以及本服务的 PERF 日志埋点（`marshal_ms`/`write_ms`/`es_ms`/`unmarshal_ms`）。
> 本文件只列 TAP 侧可做的事；JobLens 可视化侧的配套改动见 joblens_front 仓库 `docs/performance/`。

## 0. 实测基线（TAP 在整条链路中的位置）

| 观测 | 数值 |
|---|---|
| JobLens 单图端到端（job_io 1h，82.26） | 1.46s，其中 TAP 直查 0.29s（~20%） |
| PIT 单页（256 条） | ES 查询 33~51ms + 反序列化 12~19ms |
| 流式 1h new_io（720 条 / 3.8MB） | duration 853ms：**marshal 130ms + write 129ms（合计 30%）** |
| timeseries cpumem 1h | 0.031s / 154KB |
| timeseries new_io 速率 1h | 0.022s / 173KB |

结论：TAP 不是当前端到端主瓶颈（后端处理占 ~80%），但**流式路径自身的序列化/翻页开销**有明确的
低成本优化空间；时序路径的语义问题（空桶）会直接污染下游聚合结果。

---

## 一、流式 raw 路径

### P0-1 消除每条消息的重复 JSON 序列化

- **现象**：每条 `StreamMessage` 被 `json.Marshal` **两次**——一次用于字节预算计量，
  一次用于实际写出。
  - 计量：`internal/handler/stream_loop.go:102`（`data, err := json.Marshal(msg)`）
  - NDJSON 写出：`internal/handler/stream_writer.go:68`（`json.Marshal(msg)`）
  - SSE 路径同样双重：`stream_loop.go:102` 整包 marshal 计量，`stream_writer.go:54` 再 marshal `msg.Data`
- **证据**：流式 1h new_io 的 `marshal_ms=130`（占 duration 853ms 的 15%）；大 payload 场景线性放大。
- **改法**：loop 内只 marshal 一次，把 `data []byte` 直接传给 writer（NDJSON 原样写出；
  SSE 需要 payload 时从同一 map 复用或按需拆分）。writer 增加
  `writeEncoded(msg, data)` 入口；`writeSSE` 的 payload 可改为对已编码 body 做一次
  轻量抽取，或接受 SSE 路径单独一次编码（SSE 无 NDJSON 的整包需求）。
- **预期**：流式路径 −10~15% 总时长；实现 <半天。
- **风险**：低。注意 `progress.bytes` 语义保持（用同一份编码长度）。
- **实测（2026-09-18，83.20，ABBA 交叉 3 轮）**：
  - 实现：`stream_writer.go` 新增 `writeEncoded(data []byte)`，loop 的预算编码结果直接写出；
    实际改动 2 文件 23 行，SSE 保持单独编码（符合原建议的备选）。
  - fs_metadata 超宽（~722KB/条，40 条/28.9MB）：duration **2589→1842ms（−28.9%）**，write 708→20ms（−97%）；
  - new_io 中宽（720 条/8.15MB）：duration **796→644ms（−19.1%）**，write 137→7ms（−95%）；
  - cpumem 窄（17,280 条/68 页）：duration 2182→2167ms（−0.7%），write 53→11ms（−80%），
    端到端被 ES 串行翻页主导；
  - fs_metadata ×4 并发：单流 6.2→4.4s（−29%），聚合吞吐 18.6→26.3 MB/s（**+41%**）；
  - 正确性：改动前后 records 内容 md5 完全一致。
  - 结论：收益与消息体积正相关；"−10~15%"仅对中宽以上文档成立，窄文档应表述为"编码段 −80%，端到端 ~−1%"。

### P0-2 raw 翻页串行、无预取

- **现象**：`mergeRawStream` 在每次 `advance` 后才对下一个迭代器调用 `ensure(ctx)`
  （`internal/service/stream_merge.go:100`、`:147`），单迭代器下即"取一页→消费完→取下一页"，
  ES 往返延迟（33~51ms/页）直接叠加进流总时长；多集群只是在堆内交替，也不并行取页。
- **改法（二选一或组合）**：
  1. 每个 `rawStreamIter` 增加"预取下一页"（`ensure` 在消费当前页过半时异步触发），
     受每集群 1 页的额外内存上限约束；
  2. 提高请求 `page_size`（当前 JobLens 传 256；`TAP_STREAM_PAGE_SIZE` 默认 500，
     上限 `TAP_MAX_SIZE` 10000）——实测 1h new_io 从 256 调到 2000 页数 3→1，
     TAP 侧总时长 0.177s→0.160s（约 −10%，收益有限但零代码）。
- **预期**：翻页占流时长的比例越高收益越大（大窗口 raw 明显）；预取实现 1~2 天。
- **风险**：中（并发取页需保证 search_after 游标单调、PIT 生命周期、内存上限）。
- **实测（2026-09-18，83.20，ABBA 交叉 2~3 轮；P0-1 已包含）**：
  - 实现：`stream_pit.go` 加载页 i 后立即异步预取页 i+1（`pending` 双缓冲，后台完成
    SearchPIT + flatten），切页时应用；错误延迟返回、ctx 取消不泄漏；新增
    `TestStreamRawPrefetchesNextPage` 锁定预取时序，`go test -race ./...` 全绿。
  - cpumem 24h p256（68 页）：non-ES 时间（duration−Σes）**160→75ms（−53%）**，
    消费侧几乎全部与 ES 重叠；端到端约 **−4%（−85ms）**，其中 ES 分页 1.9s（93%）
    为 search_after 数据依赖的串行时间，预取无法压缩；残余 75ms 为流启动固定开销
    （OpenPIT + meta 同步写出）。
  - new_io 1h p256（3 页）：−19%（仅 P0-1）→ **−28%**（叠加预取）。
  - fs_metadata 20m p4（11 页、722KB/条）：−29%（仅 P0-1）→ **−45%**（叠加预取，
    后台 ES/unmarshal 与主线程编码在 2 核上真正并行）。
  - cpumem ×4 并发：−1.3%（ES/带宽主导，噪声内）。
  - 代价：每流额外一页缓冲（page_size=5000 约 +1.2MB；8MiB 页上限时最多 +8MiB）。
  - **预期修正**：预取收益上限由"消费侧占 duration 的比例"决定，而非翻页比例；
    窄文档大窗口的 ES 串行分页不可压缩，页大小才是该场景最大杠杆——
    A1 p256→p2000 duration 2387→1735ms（−27%）、p256→p5000 2387→1399ms（−41%）；
    调大页需注意 8MiB 页预算（new_io p1000 实测 413）。
  - **页预算与内存实测（2026-09-18 补充，64MiB 预算 + 8GiB 内存上限）**：
    `TAP_STREAM_PAGE_MAX_BYTES` 8MiB→64MiB 的端到端收益 **≈ 0**——唯一被解锁的
    cpumem p10000（ES 响应 9.5MiB）与 8MiB 预算内即可用的 p5000（4.5MiB）同速（~1.3s）；
    fs_metadata p4→p16/p40 反而慢 38%/100%（单页过大破坏预取流水线、ES 侧串行）；
    大页 + 宽文档 ×4 并发全程内存峰值仅 **793MB**，4GiB 上限已有 5 倍余量。
    结论：**保持 8MiB 默认，不要全局放大页**；按文档宽度协商页大小
    （cpumem/io/net 2000~5000；new_io 256~500；fs_metadata 4~8），各档均在 8MiB 内。

### P1-1 PIT 整页读入内存

- **现象**：`repository/pit.go:87` 用 `io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))`
  整页读入后再 `json.Unmarshal`（`:111`），单页上限 8MiB 常驻，且是两次解析
  （wire 结构 + 业务转换）。
- **改法**：`json.NewDecoder(resp.Body).Decode(&wire)` 流式解码（保底 LimitReader），
  或按行/按 hit 解码；大页场景可显著降峰值内存与一次拷贝。
- **预期**：单页内存峰值 −1 份副本；GC 压力下降（对高并发流式更有意义）。

### P1-2 默认不做 `_source` 投影

- **现象**：`BuildRawQuery`（`internal/service/query.go:100-130`）仅在调用方显式传
  `fields` 时设置 `_source.includes`；默认返回整条文档（含全部 files/processes 大数组）。
  JobLens 已显式传 fields，但其他调用方/调试场景容易误用。
- **改法**：文档强化（已在 API_zh.md 提示），或提供 `fields` 缺省时的保守投影开关；
  不建议改默认行为（会破坏既有调用方预期）。
- **风险**：低，属文档/运维层面。

---

## 二、时序聚合路径（`/data/timeseries/stream`）

### P0-3 空桶输出 0 污染聚合语义（下游已实际踩坑）

- **现象**：查询固定 `min_doc_count: 0`（`internal/service/query.go:612`、`:722`），
  ES 对空桶返回 `value: null`；`extractMetricValue`（`query.go:840-848`）对 null 做
  类型断言失败后返回**零值 0**，于是"空桶"与"真实 0 值"不可区分。
- **影响**：JobLens 详情图用 min/max 包络呈现；空桶 0 会污染 min 聚合
  （0 成为最小值），当前只能把"两方向均为 0"的桶按缺失丢弃——**真实 0 值（空闲时段）
  同样被丢弃**，图表出现断点。见 joblens_front `docs/performance/after-es-aggregation.md` §4。
- **改法（任一）**：
  1. **跳过空桶**：解析时识别 `doc_count == 0` 或 `value == null`，不产出 record
     （推荐；`date_histogram` 的空桶 bucket 里本就有 `doc_count` 字段可判）；
  2. 保留空桶但输出 `null`（需要 JSON null 语义 + 下游适配）；
  3. 查询参数化 `min_doc_count`（默认 1，调用方显式要补零时才用 0）。
- **预期**：修复下游聚合的正确性缺陷（当前靠近似规避）；实现 <半天。
- **兼容**：方案 1/3 改变"补零网格"的既有行为，需检查现有调用方是否依赖全网格输出
  （JobLens 前端按 timestamp 数组消费，跳过空桶不影响；文档需同步更新）。

### P1-3 窗口 × collector 全串行

- **现象**：`StreamTimeSeries` 外层窗口循环内层 collector 循环，逐个
  `streamAggregationWindow`（`internal/service/stream_timeseries.go:121-128`），
  串行 ES 请求数 = 窗口数 × collector 数；大窗口（多窗口切分）时延迟线性叠加。
- **改法**：按窗口/collector 并发（有界，如 3~4），保持输出顺序与 done 统计语义
  （可先并发取数、再按窗口序输出）；或按窗口水平分片由调用方并发（JobLens 侧已可做）。
- **预期**：多窗口查询（如 24h/7d）时序出流时间显著下降。
- **风险**：中（顺序契约、内存、`StreamMaxWindows` 上限）。

### P1-4 单请求多聚合方向（min+max 组合）

- **现象**：下游要画 min/max 包络时必须发**两次查询**（`agg=min` 与 `agg=max`），
  往返翻倍且当前 JobLens 实现为串行——这是 ES 聚合方案在 2 核环境负收益的原因之一
  （见 joblens_front `docs/performance/aggregation-and-parallel-results.md` §1）。
- **改法**：支持一次请求返回多 agg（如 `agg=min,max` 或 `agg=minmax`，响应 record
  增加 `agg` 维度）；`BuildMultiMetricTimeSeriesQuery` 的 `agg_<metric>` 聚合复制为多份。
- **预期**：下游聚合查询次数减半；TAP 侧无额外成本（同一批桶多算一列）。
- **风险**：中（响应结构 additive 变更，需协议版本标注）。

---

## 三、registry 与索引解析

### P1-5 未知 metric 导致索引解析退化

- **现象**：`InferCollectorFromMetric`（`internal/model/registry.go:408-412`）对未注册
  别名返回空串；`GroupMetricsByCollector` 将其归入 `""`，`streamAggregationWindow`
  以默认采集器列表调用 `ResolveIndices`（`internal/service/index.go:30-40`）——
  即**未知 metric 会去查询所有采集器索引**（放大 ES 压力，且可能命中无关索引）。
- **改法**：metric 既不是别名也不是合法 ES 字段路径时返回 400（明确报错），
  或显式记录"全采集器"语义并计入限流。
- **预期**：消除误用放大；实现 <半天。

### P2-1 索引按天展开

- **现象**：`ResolveIndices` 对 [from,to] 逐日生成索引（`index.go:42-52`），7 天 × N 采集器
  的索引列表可观；`full_range` 走通配符（大量分片查询）。
- **改法**：索引名缓存/别名化（ES 侧 index template + 别名），或对连续多日查询使用
  `collector_*` 通配 + 时间过滤（让 ES 自行裁剪分片）。
- **风险**：低收益/中成本，视集群分片规模评估。

---

## 四、非流式路径（对照项）

### P2-2 `track_total_hits` 与通用 map 解码

- **现象**：非流式 `Search` 固定 `WithTrackTotalHits(true)`（`internal/repository/es.go:201`），
  大结果集下统计总命中成本高；响应先解码为 `map[string]any` 再二次遍历
  （`es.go:246`、`:360`），GC 压力大（PIT 流式路径已用 typed 解码且关闭总命中统计）。
- **改法**：非流式路径也按需关闭/限制 `track_total_hits`（`size` 上限已知时无意义）；
  改用 typed wire 结构解码。
- **预期**：非流式大查询延迟/内存下降；流式路径已优化，非流式主要服务 ad-hoc 查询。

### P2-3 flatten 数组按下标展开

- **现象**：`flattenNested`（`internal/service/flatten.go:159`）对数组按元素下标展开
  键名（`data.files.0.rchar`，`:171`），字段数上限 `TAP_MAX_FLATTEN_FIELDS=2000`；
  大数组易触顶且键名膨胀。
- **改法**：默认流式已不 flatten（`flatten=false`）；非流式可考虑"数组摘要"模式
  （只展开前 N 个元素）或文档化上限行为。
- **风险**：低（属既有契约，改动需谨慎）。

---

## 五、连接与限流

### P2-4 ES 连接池参数

- **现象**：`es.go:90-103`：`MaxIdleConnsPerHost=10`、无 `MaxConnsPerHost` 上限、
  `ResponseHeaderTimeout=30s`。
- **改法**：按并发流上限（`TAP_MAX_CONCURRENT_STREAMS=16`）评估连接与排队；
  可加 `MaxConnsPerHost` 防止极端放大（PIT + 多窗口并发时）。
- **预期**：稳定性/尾延迟改善，非吞吐关键路径。

### P2-5 全局流式限流

- **现象**：`internal/service/limiter.go` 单一全局信号量（默认 16），非按用户/集群/租户。
- **改法**：如需多租户公平性，可扩展为分层限流；当前规模（单 JobLens 调用方）够用，
  可暂不做。

---

## 六、观测性（已有良好基础）

已有 `PERF pit/page/stream` 埋点（es_ms、flatten_ms、unmarshal_ms、marshal_ms、write_ms、
vm_rss/vm_hwm）与可选 `TAP_PPROF_ADDR`。建议补充：

1. **慢查询日志**：超过阈值（如 ES >200ms 或单页 >8MiB）打 WARN，含 cluster/job/collector/indices；
2. **翻页统计**：每流的页数、平均页耗时（评估 P0-2 预取收益）；
3. **空桶比例**：timeseries 响应中空桶/总桶占比（P0-3 的影响面评估）。

---

## 七、优先级汇总

| 编号 | 项 | 类型 | 预期收益 | 成本 | 风险 |
|---|---|---|---|---|---|
| P0-1 | 消除每条消息双 marshal | 性能 | **实测**：宽 −29% / 中 −19% / 窄 −1%；宽文档并发吞吐 +41% | 0.5 天 | 低 |
| P0-2 | 翻页预取（含 page_size 调优） | 性能 | **实测**：宽 −16pp / 中 −9pp / 窄 −4%；页 256→2000 −27% | 1~2 天 | 中 |
| P0-3 | 空桶语义（跳过/暴露 null） | 正确性 | 修复下游聚合缺陷 | 0.5 天 | 中（契约） |
| P1-1 | PIT 流式解码 | 内存 | 峰值 −1 副本 | 0.5 天 | 低 |
| P1-3 | 时序窗口并发 | 性能 | 多窗口下降 | 1~2 天 | 中 |
| P1-4 | 多 agg 一次返回 | 接口 | 下游查询减半 | 1 天 | 中 |
| P1-5 | 未知 metric 显式报错 | 健壮性 | 消除索引退化 | 0.5 天 | 低 |
| P2-1 | 索引别名/缓存 | 运维 | 视规模 | 1~2 天 | 低 |
| P2-2 | 非流式 track_total_hits/typed | 性能 | ad-hoc 查询 | 1 天 | 低 |
| P2-4 | 连接池上限 | 稳定 | 尾延迟 | 0.5 天 | 低 |

> P0-1、P0-2 已于 2026-09-18 在 83.20 完成实现与 ABBA 对比实测（报告：
> 工作区 `reports/perf-baseline-2026-09-18/`，未纳入版本库）；P0-2 余下的
> page_size 调优属调用方参数，未改服务端默认值。

## 八、验证方法

- 每次改动后用 `PERF stream` 的 `marshal_ms/write_ms/duration_ms` 前后对比
  （同作业同窗口，见 joblens_front `docs/performance/benchmark-method.md` 的 TAP 直查方法）；
- 预取类改动（P0-2）用 **non-ES 时间（duration−Σes_ms）** 评估，端到端 duration 会被
  ES 侧 ±400ms 波动淹没（单流收益仅 ~85ms）；对比实验需用 ABBA 交叉消除顺序效应；
- P0-3 用同作业空桶时段验证：修复后空桶不应出现在 records 中；
- P0-2 用 24h raw（大页数）对比总时长与页耗时分布；
- 回归：`go test ./...`，重点 `internal/service/stream_*_test.go`、
  `stream_prefetch_test.go`、`stream_windows_integration_test.go`、`alias_drive_test.go`。
