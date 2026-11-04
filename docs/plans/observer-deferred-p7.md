# P7 实现方案：observer 边界 / 30 分钟批处理

## 影响分析

- 共享入口为 `http/shared.ts`：observation / summarize 在同步入队后才确认 hook-spool handoff，随后唤醒 `SessionRoutes.ensureGeneratorRunning`。
- `SessionManager` 管理所有 provider 的内存 buffer 与确认流程；仅拦 generator 启动无法阻止运行中的 drain。
- `SessionEnd` 当前只请求 Telegram wrapup，不触发 observer。本次启用延迟模式时显式释放并排入总结，默认模式不变。
- `worker-service.ts` 在数据库与路由就绪后显式初始化 opt-in 调度器。测试不自动访问用户数据库。

## 技术方案

1. 新增 `DeferredObserverQueue`，在现有数据库建立独立 outbox 表；waiting 项不进入 SessionMessageBuffer，边界 / 最老事件绝对期限释放快照。
2. 分离 waiting / released / inflight / parked / done：恢复 waiting、released，启动时把旧 inflight 隔离为 parked。完成去掉 payload、保留去重身份。绝不复活旧 pending_messages 队列。
3. 现有 buffer ID 映射 outbox ID，真正 yield 时标记 inflight，确认后完成。后续事件仍进等待 outbox，不可绕过运行中的 generator。
4. 已有 provider batching / paid-send-budget 不修改；16 是配置上限，不参与调度触发。
5. 设置默认 immediate；最长等待默认 30min，0 为无限等待。内部 mem 工具过滤只针对自己的 MCP server，不屏蔽业务工具及显式写入。
6. outbox 外键关联 sdk_sessions 并 ON DELETE CASCADE，兼容现有 session 删除及隐私清理；总结身份包含最近观察边界，既去重重发又允许同 prompt 的合法后续总结。

## 风险与限制

- 崩溃时结果不确定的 inflight 项保留为 parked，不自动重发；可能需要人工核查，而不是隐式重复收费。
- 调度模式与持久化恢复在 worker 初始化时应用，需要服务重启才改变模式；max-wait 不是不断重置的空闲窗口。
- flush 可包含 init、多个有限批次和 summary，不能保证单轮只有一次 API 请求。

## 验证

用模拟时钟与临时 SQLite 测试 A1-A8；检查默认即时模式与现有 observer / session-end / paid-send-budget 回归。P8 负责独立验收、构建与停止状态确认。禁止任何真实模型、worker、Claude Code 启动。

## P7 自审与执行证据

- 接口：现有 SessionManager 构造签名不变，新 configureDeferredObserver 仅在 worker 的数据库初始化后启用。所有 provider 仍消费同一个内存 buffer，默认 immediate 不建 outbox。
- 边界：覆盖无限等待、老时间戳、重复边界、SQLite 重开、结果不确定隔离、显式取消、session 删除级联、timer 已完成观察后 SessionEnd 补总结、付费预算耗尽停车，以及新事件不进入已释放批次。
- Proper fix：把等待和可消费工作物理分离，没有通过只拦首次 generator 或放长网络超时来掩盖问题；沿用已有预算/批处理限制。
- 已执行新逻辑定向测试 18 项通过；旧 Claude batch、session-end route、memory retrieval 与设置验证在依赖就绪后复跑通过。具体汇总由 P8 的独立验收报告记录。
- 首次 SQLite 重开测试的 Windows 临时文件清理出现 EBUSY，使用 Bun.gc 释放 SQLite statement 与有限清理重试后通过，逻辑断言未放宽。
- 未执行真实模型、服务启动或用户数据库操作。构建及安装同步归 P8。
