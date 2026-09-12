# 查询护栏与流式接口参数调优指南

本文说明如何为 `/data/raw/stream` 和 `/data/timeseries/stream` 选择参数，并验证内存、响应时间与结果完整性。适用于当前实现；参数默认值以 `internal/config/config.go` 为准，接口契约见 [API 文档](API_zh.md#31-流式查询超大结果)。

**推荐顺序：固定负载 → 裁剪字段 → 调单页/单窗 → 调并发 → 调超时与续传 → 持续验证。** 不要同时放大页大小、并发数与字节上限。

## 1. 先明确调优目标

在测试前确定以下指标的目标值：

| 指标 | 含义 | 注意事项 |
|---|---|---|
| 首字节时间 TTFB | 客户端收到响应头/首字节的时间 | TAP 可以先发送 meta；TTFB 小不代表数据查询快 |
| 首批数据时间 | 收到第一条 `records` 消息的时间 | 用于衡量流式查询的可用等待时间 |
| 总响应时间 | 从请求发出到响应体接收完成 | 要包括最后一条终态消息，不能只测 headers |
| 完整成功率 | 收到 `done.complete=true` 且无错误的请求比例 | HTTP 200 不等于完整成功 |
| 预算截断率 | 因 `record_limit` / `byte_limit` 结束的比例 | 如果目标是完整导出，预算截断不算成功 |
| 拒绝/后端错误率 | HTTP 429、流内错误等 | 与正常查询延迟分别统计 |
| 峰值 RSS | TAP 进程实际驻留内存峰值 | 不等于 Go heap，也不等于输出字节数 |
| 吞吐 | 成功写出的记录数或字节数 / 场景墙钟时间 | 同时报告请求数及完整性，避免失败请求抬高吞吐 |

记录接口、计算集群、作业、采集器、字段选择、时间范围、分组方式、页大小、窗口大小、并发与请求上限。使用固定绝对时间，避免 `now` 在不同轮次改变测试数据。

**ES 集群名与计算集群名不是同一概念。** 例如 `omat4htc` ES 中的数据可能来自 `condorce02.ihep.ac.cn`。TAP 的 `cluster` 必须解析到文档中的计算集群名以及正确的 tag/routing，不能把空结果当作性能提升。

## 2. 参数及相互约束

### 2.1 页、窗口与结果预算

| 参数 | 当前默认值 | 作用与调优方向 |
|---|---:|---|
| 请求 `fields` | 不裁剪 | 优先选择真正需要的字段，减少 ES 响应、解码和输出开销；同时验证返回结构符合消费者预期 |
| `TAP_STREAM_PAGE_SIZE` / 请求 `page_size` | 500 | Raw 每次拉取的文档数。宽文档先从 50–100 开始试验 |
| `TAP_MAX_SIZE` | 10000 | Raw 页大小上限；不是推荐页大小 |
| `TAP_STREAM_PAGE_MAX_BYTES` | 8388608（8 MiB） | 单次流式 ES 响应体的读取上限；超过时返回 `response_too_large`，并不保证解码后的内存只有 8 MiB |
| `TAP_STREAM_WINDOW_BUCKETS` / 请求 `window_buckets` | 5000 | 时序单窗日期桶的目标上限，实际还会被桶数预估预算压低 |
| `TAP_MAX_ES_BUCKETS` | 65536 | TAP 桶数预估限制；应结合目标 ES 的实际配置设置，不能假设所有 ES 都采用默认值 |
| `TAP_MAX_METRICS` | 20 | 单次时序指标数上限；更多指标会增加输出记录与聚合开销 |
| `TAP_STREAM_MAX_WINDOWS` | 2000 | 时间窗口规划上限，超限在分配窗口切片前拒绝 |
| `TAP_STREAM_MAX_RECORDS` / 请求 `max_records` | 100000 | 单流记录数硬上限；请求参数只能降低服务端限制 |
| `TAP_STREAM_MAX_BYTES` | 67108864（64 MiB） | 单流中间消息的编码预算，超过时返回 `byte_limit`；不是进程 RSS 限制 |
| `TAP_MAX_FLATTEN_FIELDS` | 2000 | 单条记录扁平化键数上限；降低它会影响字段完整性，不能代替 `fields` 裁剪 |

当前字节计量有一个已知边界：`done.bytes` 按中间消息的 NDJSON 编码长度累计，未完整计入终态、心跳及 SSE 帧格式差异。因此它不是精确网络字节数，64 MiB 也不是最终响应体的严格线速上界。压测应使用客户端实际接收字节数。

达到预算时以整批边界决定是否输出：即使还有剩余字节预算，下一批放不下也会停止。缩小页大小能降低这种预算尾部浪费，但会增加 ES 请求次数。

### 2.2 并发、超时与恢复

| 参数 | 当前默认值 | 作用 |
|---|---:|---|
| `TAP_MAX_CONCURRENT_STREAMS` | 16 | 同时执行的流式请求数，超出直接 HTTP 429；不覆盖所有普通查询 |
| `TAP_STREAM_MAX_CLUSTERS` | 16 | 单次流式请求最多访问的计算集群数；多集群归并会同时保留多个输入页 |
| `TAP_QUERY_TIMEOUT` | 60s | 单次 ES 查询上下文超时 |
| `TAP_STREAM_TIMEOUT` | 10m | 整个流的上下文截止时间 |
| `TAP_STREAM_WRITE_TIMEOUT` | 30s | 每次写入前刷新写 deadline，用于限制阻塞写入 |
| `TAP_SSE_HEARTBEAT` | 15s | 流已开始后发送心跳的间隔；不会延长整体流超时 |
| `TAP_STREAM_PIT_KEEP_ALIVE` | 2m | Raw PIT 的租约，每次成功查询续期；影响断线恢复窗口与 ES 资源保留时间 |
| `TAP_STREAM_CURSOR_KEY` | 进程随机生成 | 游标签名密钥。多实例或需要跨重启恢复时设置一致的稳定密钥，但仍受 PIT 有效期约束 |

配置由环境变量在启动时读取，修改后需重启目标 TAP 进程。当前 `Normalize()` 对上述多数非正数配置恢复默认值，**不能用 `0` 表示关闭限制或关闭心跳**。

当前 ES Transport 另有 30s `ResponseHeaderTimeout`（`internal/repository/es.go`）；只增大 `TAP_QUERY_TIMEOUT` 不会同步扩大这个传输层限制。整体超时也不等于所有初始化、阻塞写入和清理操作都能在该瞬间结束，必须用客户端墙钟实测。

## 3. Raw 调优：先控制单页内存

### 第一步：选择有代表性的数据

至少准备三类负载：

1. 只查询 `fields=cpu,mem` 等少量字段的窄记录。
2. 包含完整进程/线程/文件数组的宽记录。
3. 最大允许集群数或接近该数量的多集群查询。

同一作业不同时段的文档宽度也会变化。不能用第一条文档大小推断全部页都能通过 8 MiB 限制。

### 第二步：估计起始页大小

用小样本观察 ES 原始响应中每条 hit 的体积，优先使用高分位体积而非均值。可用下式作为起点：

```text
候选页大小 ≈ 单页响应预算 × 预留比例 / 每条 hit 的高分位字节数
```

例如，单页预算 8 MiB、预留比例取 0.5、每条 hit 约 16 KiB，起点约为 256 条。**预留比例是实验参数，不是实现保证**；必须考虑 ES 外层 JSON、sort、字段变化，以及解码后的对象放大。

随后固定并发为 1，依次试验 `page_size=50,100,250,500`。每档记录首批时间、完整性、峰值 RSS 与总吞吐。宽文档不要直接跳到 2000 或 10000。

遇到 `response_too_large` 时依次尝试：

1. 裁剪 `fields`。
2. 降低 `page_size`。
3. 核实是否单条文档已经超过单页字节预算。
4. 仅在测得足够内存余量后，小幅增加单页预算并重跑并发测试。

缩短时间范围通常能减少总输出，但不一定减少一个满页的大小；它不能替代单页控制。

### 第三步：确认完整导出是否允许

粗估总输出量：

```text
总输出量 ≈ 匹配记录数 × 平均输出记录字节数 + 消息开销
```

若明显超过总记录/字节预算，应选择字段裁剪、分次读取并续传，或按不重叠范围拆分任务。不要把 `done.status=success` 单独当作完整导出：当前记录上限结束也可能是 `status=success`，但 `complete=false`、`stop_reason=record_limit`。

## 4. 时序调优：减少不必要的桶和下游调用

先确定业务需要的 `interval`，再选择 `window_buckets`。例如只用于一天范围的概览图时，1 分钟粒度通常比每秒粒度产生更少的点；是否允许降采样应由业务需求决定，不能为跑分快而改变统计目标。

当前无分组单指标查询，每个日期桶输出一条记录。多个指标及分组会放大输出规模：

```text
输出记录数约为 日期桶数 × 指标数 × 有效分组数
```

这是容量估计，不是 ES 扫描成本，也不是 ES `search.max_buckets` 的精确定义。标量指标并不等于桶；nested、分组和不同采集器会影响实际 DSL。

建议固定范围/指标/分组，试验 `window_buckets=100,500,1000,5000`：

- 太小：首批可能更快，但 ES 往返次数增多，总耗时变长。
- 太大：单次聚合与响应体增大，可能触发桶数、页字节或超时限制。
- 实际单窗大小由 `windowDateBuckets` 再限制，不能只看请求参数。
- 内部窗口按 ES UTC 固定桶网格对齐；末端保留包含 `to` 的语义。`to` 正好位于分窗边界时可能多出一个末端窗口，应以 `meta.windows` 为准。
- 分组流当前超过 terms 上限时可返回 `group_limit`；扩大窗口或 `TAP_MAX_METRICS` 不能消除分组完整性限制。

每次改变窗口大小后，与相同请求的普通时序结果比较：桶键集合、重复/缺失、数值相对误差、全局统计。平均值由数值 `sum/count` 合并；允许浮点舍入误差，不应出现明显统计偏差。

## 5. 并发调优：找到吞吐拐点

选定单页/单窗后，再试验并发 `1 → 2 → 4 → 8 → 16`，同时保留普通查询与健康检查作为旁路探针。

每档固定请求数量或持续时间，记录成功率、预算截断率、429 数量、首批/总耗时 P50/P95/P99、峰值 RSS、CPU、ES 查询耗时与拒绝情况。初筛至少多轮重复；候选配置再做足够长的持续负载，检查 GC、PIT 租约积累和内存恢复情况。时间长度、样本量与停止阈值由实际资源预算确定。

**当并发增加但吞吐不再增长，而延迟/RSS继续上升时，选择前一档附近作为候选。** 不能仅因为所有请求最终返回 200 就继续加并发。

可用以下估计检查内存余量：

```text
可用于并发流的内存 = 进程/容器预算 − 空闲基线 − 普通查询开销 − 安全余量
每流增量估计 = (某档峰值 RSS − 空闲 RSS) / 活跃流数
候选并发上界 ≈ 可用于并发流的内存 / 每流增量估计
```

此处只是经验估算。GC、页宽、预取及序列化重叠会破坏线性关系；多集群请求还会放大页缓冲。必须用混合负载验证，不能把 `并发 × 单页字节预算` 当作 RSS 上界。

建议给负载发生器单独分配 CPU/机器。若与 TAP 同机，记录客户端 CPU 与解析开销，避免把负载发生器瓶颈误认为服务器吞吐极限。进行页大小 A/B 对比时交替测试顺序，并分别报告冷启动与热缓存结果。

## 6. 超时、心跳与 PIT 租约

1. 先找出耗时来自初始化、单次 ES、窗口数量还是客户端消费；不要统一调大所有超时。
2. 单次查询超时应覆盖正常的 ES 请求尾延迟；整体超时覆盖允许的总查询时长。嵌套 context 会受较早的 deadline 限制，但传输层还有独立限制。
3. 写入超时用于防止客户端长期不读取。每次写入重置不是无限续命：整体流超时仍然存在。
4. 心跳间隔应小于链路中最短的代理空闲超时；核实代理是否遵守 `X-Accel-Buffering: no`。只测直连不能证明代理部署也流式。
5. PIT 租约覆盖下次取页间隔、客户端处理时间和允许的恢复延迟。延长租约会延长 ES 对旧段等资源的保留，需观察 ES 的搜索上下文及资源情况。
6. 单集群 raw 可能为续传保留 PIT 到 TTL；重复压测应覆盖租约积累周期。多实例需使用一致的 `TAP_STREAM_CURSOR_KEY`，密钥不要写入报告或仓库。
7. SSE 消费者收到终态后主动 `close()`；只有已经确认完成的终态游标重连才可返回 204。`cursor_expired` 不能当作空数据成功。

## 7. 可重复的本地测量步骤

以下命令从仓库根目录执行。需要 Go、curl、jq；`TAP_MANAGEMENT_API_URL` 应指向可访问且包含目标计算集群的元数据服务，凭据由既有配置提供。

### 7.1 编译并保存运行身份

```bash
RUN_DIR=$(mktemp -d /tmp/tap-tuning.XXXXXX)
go build -o "$RUN_DIR/tap-server" ./cmd/server
go version > "$RUN_DIR/environment.txt"
uname -sr >> "$RUN_DIR/environment.txt"
sha256sum "$RUN_DIR/tap-server" >> "$RUN_DIR/environment.txt"

export TAP_MANAGEMENT_API_URL="http://<management-host>"
export TAP_COLLECTOR_REGISTRY_PATH="$PWD/collector-registry.json"
export TAP_PORT=18089
export TAP_LOG_LEVEL=warn
export TAP_STREAM_PAGE_SIZE=100
export TAP_MAX_CONCURRENT_STREAMS=4

"$RUN_DIR/tap-server" > "$RUN_DIR/server.log" 2>&1 &
TAP_PID=$!
```

确认 `/health` 后先执行一条有结果的真实查询；`/ready` 在尚未创建 ES 客户端时不能独立证明目标 ES 可查询。

### 7.2 测量单请求及完整性

```bash
curl -sS -N --max-time 120 \
  -D "$RUN_DIR/headers.txt" \
  -o "$RUN_DIR/response.ndjson" \
  -w 'http=%{http_code} ttfb_s=%{time_starttransfer} total_s=%{time_total} bytes=%{size_download}\n' \
  --get 'http://127.0.0.1:18089/data/raw/stream' \
  --data-urlencode 'cluster=<计算集群名或别名>' \
  --data-urlencode 'job=<NativeJobID>' \
  --data-urlencode 'collector=cpumem' \
  --data-urlencode 'from=2026-09-11T00:00:00Z' \
  --data-urlencode 'to=2026-09-11T23:59:59Z' \
  --data-urlencode 'fields=cpu,mem' \
  --data-urlencode 'page_size=100' \
  --data-urlencode 'max_records=3000'

jq -c 'select(.type == "error" or .type == "done")' "$RUN_DIR/response.ndjson"
```

curl 的 `time_starttransfer` 不是首批数据时间。正式负载脚本应增量解析 NDJSON/SSE，在第一条 `records` 到达时打点；同时记录 HTTP 状态、所有流内错误、终态和客户端异常。不要用必须把整个响应读入内存的解析方式测大导出。

无 `done` 的 EOF、客户端超时、无数据的失败请求均单独分类。已写出的计数不等于客户端业务已持久化的数量。压测响应文件可能包含业务数据与游标，按内部数据管理要求保存，不提交仓库。

### 7.3 测量进程内存

```bash
grep -E '^(VmRSS|VmHWM):' "/proc/$TAP_PID/status"
```

`VmRSS` 是当前驻留内存，`VmHWM` 是该进程生命周期的 RSS 高水位，单位为 KiB。测试完成但进程退出前记录 `VmHWM`。每个对比场景重启 TAP，避免上一场景的高水位污染结果；热缓存吞吐则另做不重启的持续场景。

建议负载脚本每 20–100ms 保存一次 `/proc/$TAP_PID/status`，并记录请求起止时间。短暂峰值可能漏采，所以最终同时报告采样峰值与内核高水位。容器环境还应采集 cgroup 内存；它与进程 RSS 不完全相同。

```bash
kill -TERM "$TAP_PID"
wait "$TAP_PID"
```

### 7.4 验证护栏，而不只测成功路径

用独立测试实例降低限制，分别检查：

| 场景 | 期望 |
|---|---|
| `size` 超过 `TAP_MAX_SIZE` | HTTP 400 |
| `interval=1ns` | HTTP 400，不进入大窗口构建 |
| 合法间隔但窗口数超限 | HTTP 400，RSS没有与理论窗口数成比例增长 |
| 服务端记录上限 200，客户端请求 1000000 | 实际返回不超过 200，终态明确截断 |
| 降低单页/总输出字节预算 | 分别触发 `response_too_large` / `byte_limit`，不把 HTTP 200 当成功 |
| 并发限制 2，同时发出 8 个且保持前两个活跃 | 多余请求 HTTP 429；记录实际重叠情况 |
| 降低整体超时 | 在合理调度/清理余量内结束；已发送条数不归零 |
| 客户端停止读取或主动断开 | 阻塞写有界、producer退出、并发槽位释放；观察后端是否仍持续执行 |
| 单集群 raw 100条 + 续传100条 | 与独立200条基线序列一致；过期/篡改游标拒绝 |
| 相同时间范围，不同窗口大小 | 时序桶键无重复/缺失，统计在浮点容差内一致 |

## 8. 2026-09-12 真实 ES 实测参考

测试二进制 SHA256：`c714a5786c2c22d643273f29e0da2c3f557aa378ed3bfaf255ae0e4074de17b8`。环境为本地 WSL2、Go 1.25、32逻辑CPU，真实 ES `omat4htc` 9.1.3。计算集群 `condorce02.ihep.ac.cn`，作业 `2864214.0`，2026-09-11 的 `cpumem` 数据共16,934条。宽记录平均约13KB；空闲TAP RSS约17–18MiB。

### 并发场景：每请求读取3000条宽记录

| 并发 | 页大小 | 请求样本数 | 总响应时间中位数 | 峰值 RSS | 总吞吐 |
|---:|---:|---:|---:|---:|---:|
| 1 | 500 | 2 | 1.78s | 166.9MiB | 1441条/s |
| 4 | 500 | 8 | 3.07s | 520.6MiB | 3809条/s |
| 8 | 500 | 16 | 6.04s | 961.9MiB | 3774条/s |
| 4 | 100 | 4 | 2.58s | 127.7MiB | 4614条/s |

这些请求都按3000条预算结束，并非完整导出。页100场景在后续轮次运行，缓存/负载未严格隔离，不能把全部吞吐差异归因于页大小。4到8并发未提高本次吞吐，提示应在更低并发附近进一步验证。

### 单请求场景

| 场景 | 总耗时 | 输出 | 峰值 RSS | 结果 |
|---|---:|---:|---:|---|
| 宽记录 page500 | 4.22s | 5000条 | 171.6MiB | 后续页超过8MiB，部分失败 |
| 宽记录 page2000 | 887ms | 0条 | 36.8MiB | 第一页被拒绝，不算性能提升 |
| 宽记录 page100 | 3.93s | 5000条 | 58.2MiB | 达到64MiB总输出预算 |
| `fields=cpu,mem`、page500 | 7.60s | 16934条 | 32.9MiB | 完整成功 |
| 1秒时序、window5000 | 728ms | 86400点 | 33.3MiB | 完整成功，18批 |
| 1分钟时序、window2 | 5.43s | 1440点 | 24.3MiB | 完整成功，720批 |

实测还确认：默认16并发限制下20个同时请求中4个返回429；720窗与普通时序结果无重复/缺失，最大相对值误差约 `2.29×10⁻¹⁶`；100+100条续传与200条基线序列一致。

这是短时阶梯实验，不是长期容量证明。Node负载发生器与TAP同机，客户端解析及共享网络会影响时间。当前测试未证明全量ID唯一性、持续高负载下PIT积累上界或所有数据形态的内存峰值。

原始测试资料保存于测试机的 `/tmp/opencode/tap-real-load-20260912/`（`REPORT.md`、`results.json`、`load.cjs`、`*-memory.json`、日志）。临时目录可能被清理；需要复用结论时应重新测量或将脱敏产物归档。该目录的 `scheme.json` 含凭据，不应提交或分享。

## 9. 宽文档的候选起始配置

以下只是由上述实测得到的保守起点，**不是所有环境的生产默认值**：

```bash
export TAP_STREAM_PAGE_SIZE=100
export TAP_MAX_CONCURRENT_STREAMS=4
export TAP_STREAM_PAGE_MAX_BYTES=8388608
export TAP_STREAM_MAX_BYTES=67108864
export TAP_STREAM_MAX_RECORDS=100000
export TAP_STREAM_WINDOW_BUCKETS=5000
export TAP_STREAM_MAX_WINDOWS=2000
export TAP_QUERY_TIMEOUT=60s
export TAP_STREAM_TIMEOUT=10m
export TAP_STREAM_WRITE_TIMEOUT=30s
export TAP_STREAM_PIT_KEEP_ALIVE=2m
export TAP_SSE_HEARTBEAT=15s
```

并发和页大小仍允许合法请求选择更大页、更多集群；如果要把候选配置变成实际容量边界，还要根据其他接口兼容性收紧 `TAP_MAX_SIZE`、`TAP_STREAM_MAX_CLUSTERS`，并测试混合负载。不要为了控制流式而无意破坏普通 raw 查询需求。

## 10. 症状与调整方向

| 症状 | 优先检查/调整 |
|---|---|
| TTFB小、首批慢 | 单页/单窗过大、ES耗时、原始文档宽度；不要只看meta时间 |
| `response_too_large` | `fields`、页大小、单条文档宽度；最后才考虑增加单页预算 |
| `byte_limit` | 是否本就无法在当前预算内完整导出；裁剪字段、续传或拆任务 |
| RSS随并发显著上升 | 降低页大小与并发，验证多集群缓冲放大；输出预算不限制RSS |
| 加并发不加吞吐 | 检查客户端CPU、TAP GC/CPU、ES线程池和网络，退回拐点前一档 |
| 小窗口首批快、总查询很慢 | ES请求数量过多；在单窗预算允许范围内增加 `window_buckets` |
| 大窗口超时 | 减少单窗桶数/指标/分组，检查ES负载与传输层timeout |
| `too_many_windows` | 增大interval，或在单窗预算允许时增大window_buckets，或缩短时间范围 |
| `group_limit` | 缩小分组范围；不是提高记录上限就能解决的问题 |
| 续传经常过期 | 客户端消费/重连间隔是否超过PIT租约；不要静默换新快照 |
| SSE不断重连 | 消费者是否收到done后close、是否读取终态；单纯延长timeout无效 |

## 11. 调优结果记录模板

每轮至少保存：

```text
日期 / 二进制SHA256 / Go版本 / ES版本 / CPU / 内存或容器预算
计算集群与routing / 作业 / 绝对时间范围 / 采集器 / fields / flatten
所有变更环境变量 / page_size / interval / window_buckets / max_records
并发 / 请求数 / 持续时间 / 预热方式 / 冷热缓存 / 负载发生器位置
HTTP状态分布 / 流内错误分布 / 完整成功数 / 预算截断数 / 无终态数
TTFB、首批数据、总耗时的P50/P95/P99 / 样本数量
总记录、实际下载字节、吞吐 / 空闲RSS / 采样峰值RSS / VmHWM
ES耗时、拒绝、PIT上下文情况 / 结果一致性验证
选用配置、理由、停止阈值 / 未覆盖场景 / 回退配置
```

最终选择应同时满足结果正确、预算可控、尾延迟可接受与吞吐目标。参数调优不能替代修复内存放大、错误计量或查询语义问题。
