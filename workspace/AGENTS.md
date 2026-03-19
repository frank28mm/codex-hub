# Codex Hub Workspace AGENTS

1. 所有 Codex 会话默认从当前 `workspace/` 根目录启动。
2. 这里是行动系统工作层，不是长期记忆库；长期记忆位于同级目录 `../memory/`。
3. `projects/` 目录用于承载真实项目的本地工作副本，不属于行动系统仓的版本管理范围。
4. 不要把 `projects/` 下的项目文件、项目源码、项目构建产物提交到 `Codex Hub` 产品仓。
5. 新会话启动时只允许读取最小入口，不允许全量扫描整个 `memory/`。
6. 先读取 [MEMORY_SYSTEM.md](/Users/frank/Codex Hub/workspace/MEMORY_SYSTEM.md)。
7. `ops/start-codex` 是最强保证的显式入口：
   - 不要求用户先手动打开 Obsidian
   - 会在需要时自动确保 sibling `memory/` 可被后台读取，并在安装了 Obsidian 时尝试把 Vault 打开在后台
   - 启动前自动发现并注册新项目
   - 启动后自动维护 `session-router.json`
   - 在项目/专题绑定后自动输出上下文建议摘要
   - 会话结束后自动写回 `memory/`
8. 如果 Codex app 直接在当前 `workspace/` 中打开新对话，这就是推荐的日常主模式，也必须遵循同一套协议，而不是退化成普通项目对话。
9. app 直开模式下，优先依赖后台守护进程 [ops/codex_session_watcher.py](/Users/frank/Codex Hub/workspace/ops/codex_session_watcher.py) 自动同步本地 session 到 `memory/`。
10. 与这套系统有关的 `memory/` 读写、项目路由、会后写回，统一遵循 repo skill `obsidian-memory-workflow`。
11. 如果当前会话发生在这个 `workspace/` 内，开始阶段应优先：
   - 使用 `obsidian-memory-workflow`
   - 按需运行 `python3 ops/codex_memory.py discover-projects`
   - 确认 watcher 已安装：`python3 ops/codex_session_watcher.py status`
   - 再进入项目路由和渐进式读取
12. 如果用户首轮消息明确提到某个项目名或别名：
   - 绑定到该项目
   - 先读 `PROJECT_REGISTRY.md`
   - 再读 `ACTIVE_PROJECTS.md`
   - 再读 `NEXT_ACTIONS.md`
   - 再读该项目的一级项目板
   - 如首轮消息明确命中某个专题，再读对应专题板
   - 再用 `python3 ops/codex_context.py suggest` 获取建议入口
   - 最后按需读该项目的摘要页
13. 如果首轮消息没有点名项目：
   - 进入通用模式
   - 只读取全局入口
   - 不自动进入项目级摘要
14. `resume` 只作为热记忆层：
   - 仅在同项目且 24 小时内的最近会话存在时优先续接
   - 不把 transcript 全文直接写入长期记忆
15. 会话结束时：
   - 启动器模式由 `ops/start-codex` 直接写回
   - app 直开模式由 session watcher 在 `task_complete` 后写回
   - 两条路径都只写基础摘要，不做强压缩
16. 每次项目写回完成后，都要顺手产生 `project_writeback` 事件，触发一次检索增量同步，并让工作区总看板自动同步。
17. 工作区总看板同步器是 [ops/codex_dashboard_sync.py](/Users/frank/Codex Hub/workspace/ops/codex_dashboard_sync.py)：
   - 即时同步由写回链路直接触发
   - 定时校准由独立 launchd 任务兜底
18. 当前事实源分层固定为：
   - 专题板：`01_working/<项目名>-<专题>-跟进板.md`
   - 一级项目板：`01_working/<项目名>-项目板.md`
   - `NEXT_ACTIONS.md`：全局动作派生板
   - `07_dashboards/`：展示层
   - 项目摘要页只保留长期背景和 `last_writeback_*`
19. 项目会话中的状态变更应优先直接落到结构化主表：
   - 专题任务改专题板
   - 项目直属任务改一级项目板
   - 不从自然语言总结自动猜测任务状态
20. `07_dashboards/` 页面只允许保留规则说明和 `AUTO_*` 机器区块：
   - 不再手写项目数、项目名单或 `todo / doing / blocked / done` 状态事实
   - 一切状态判断以自动区为准
21. `Codex Hub` 在公开版里也是正式项目：
   - 相关系统演进任务应落在 `memory/` 的 `Codex Hub-项目板`
   - 不再只靠聊天和报告追踪系统开发
22. 飞书线程规则固定为：
   - 机器人私聊是整个 `Codex Hub` 工作区的最高权限入口，不绑定单个项目
   - Feishu 群聊默认是一群一线程，但项目上下文以动态路由优先：用户直接提项目名时，机器人应像 Codex app 一样自动读项目记忆并进入该项目上下文
   - “这个群只聊 xxx”只作为可选的线程偏好，不再作为执行前置条件；同群不做多 lane
   - 项目级执行与汇报优先在项目群中完成，跨项目协调与高风险授权优先走机器人私聊
   - `CoCo` 只是示例机器人名，部署者可以改成任意名字
23. 面向管理者或外部分享的系统报告统一放在：
   - `reports/`
   - 首份系统总览报告位于：`reports/system/workspace-hub-system-overview.md`
24. 根层 `/.codex/config.toml` 和 `/.codex/rules/generated.rules` 已启用：
   - 前者只承载运行默认
   - 后者只承载命令前缀规则投影
   - 业务控制真源始终是 `control/*.yaml`
25. 权限与批准规则固定为：
   - 如果任务因为沙箱、系统权限、Git/LaunchAgent/网络或其他执行边界被拦住，不要只汇报“做不了”
   - 应先明确说明缺的是哪一类权限、当前卡在哪一步
   - 然后主动向用户申请批准或下一步授权，而不是把权限不足误写成任务失败
   - 用户一旦明确批准，后续会话应继续完成原任务，并说明哪些步骤需要重试
26. 这份 `workspace/AGENTS.md` 是运行协议，不需要用户手工替换到别处：
   - 只要 Codex 在当前 `workspace/` 下启动，它就会直接读取这里
   - 根目录的 `AGENTS.md` 只负责帮助部署这套产品，不替代运行协议
