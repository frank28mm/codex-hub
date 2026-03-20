# Codex Memory System Entry

## 固定路径

1. Workspace：
   - 当前 `workspace/` 根目录
2. Obsidian-compatible Memory：
   - 同级目录 `../memory/`

## 启动原则

1. 不要求用户先手动启动 Obsidian。
2. 当前 `workspace/` 的 Codex app 直开模式是推荐的日常主模式。
3. 标准入口 `ops/start-codex` 负责显式强制启动、排障和脚本化调用。
4. 如果 Obsidian 已安装但没有运行，标准入口会在需要时尝试在后台打开对应 Vault。
5. Codex 对记忆系统的主交互仍然是直接读写 `memory/` 文件，而不是依赖 Obsidian GUI 才能工作。
6. 如果用户直接在 Codex app 中打开当前 `workspace/` 作为工作区，`AGENTS.md`、repo skill 和 session watcher 仍应生效。
7. 当前 `workspace/` 对应的正式项目名是 `Codex Hub`，系统演进通过项目板与摘要页进入同一套记忆体系。
8. Obsidian 不是硬依赖，但如果想获得完整体验，仍然强烈建议安装：
   - 方便查看 `memory/`
   - 方便使用 `obsidian://` 深链
   - 方便把长期记忆和人类阅读界面统一起来

## Workspace 边界

1. `workspace/` 是行动系统仓，不是项目源码总仓。
2. `projects/` 目录只用于放本地项目工作副本。
3. `projects/` 下的内容默认不提交到产品仓。
4. 需要 GitHub 备份的，是：
   - 行动系统骨架
   - 启动器脚本
   - 路由规则
   - 记忆系统模板库

## 记忆激活顺序

新对话默认按以下顺序读取：

1. `AGENTS.md`
2. 本文件
3. `memory/` 根层：
   - `PROJECT_REGISTRY.md`
   - `ACTIVE_PROJECTS.md`
   - `NEXT_ACTIONS.md`
4. 当前绑定项目的一级项目板
5. 如命中专题，再读对应专题板
6. `python3 ops/codex_context.py suggest`
7. 当前绑定项目的摘要页

## 渐进式披露

固定采用四层渐进：

1. `T0`：`AGENTS.md` + `MEMORY_SYSTEM.md`
2. `T1`：`PROJECT_REGISTRY.md` + `ACTIVE_PROJECTS.md` + `NEXT_ACTIONS.md`
3. `T2`：目标项目的一级项目板；如命中专题，再加专题板
4. `T2.5`：`codex_context.py suggest` 产出的建议入口和检索命中
5. `T3`：相关决策页、规则页、系统页、人物页
6. `T4`：`02_episodic/` 和原始日志

默认只走 `T0 + T1 + T2`，项目摘要页按需读取。

## 项目绑定

1. 优先使用启动器传入的 `--project`。
2. 否则根据首轮消息中的项目名或别名绑定项目。
3. 若项目已明确，再根据首轮消息或上下文决定是否命中专题板。
4. 绑定完成后，优先运行 `python3 ops/codex_context.py suggest` 给出建议入口。
5. 若没有明确项目，则进入通用模式。

## Feishu 线程规则

1. `CoCo` 私聊固定视为整个 `Codex Hub` 工作区的管理线程：
   - 不绑定单个项目
   - 默认承接跨项目协调、主线/支线调度和高风险授权
2. Feishu 群聊固定采用：
   - 一个群 = 一条独立工作线程
   - 默认按消息内容中的项目名或别名自动路由，就像在 Codex app 里新开对话后直接提项目一样
   - `这个群只聊 xxx` 这类声明只作为可选偏好，不是执行前提
3. 不做同群多 lane，也不要求在同一个群里频繁切项目；真正的项目识别优先来自当前消息和最近上下文。
4. 需要项目级执行与汇报时，优先在对应项目群完成；需要工作区级调度或更高授权时，优先走 `CoCo` 私聊。

## 权限申请规则

1. 如果任务被沙箱、系统权限、Git/LaunchAgent 写入限制、网络限制或其他执行边界拦住，不能只停在“当前无权限”。
2. CoCo 应先明确说明：
   - 缺的是哪一类权限
   - 当前具体卡在哪一步
   - 用户批准后可以继续完成什么
3. 在可由用户补授权的场景下，应主动向用户申请批准，而不是把权限不足误判成任务失败。
4. 用户一旦明确批准，后续会话应继续沿原任务执行，并把需要重试的步骤补完。
5. Feishu 默认权限分层固定为：
   - 普通项目任务：默认执行
   - 飞书对象操作：默认执行
   - 写入 `~/.codex/skills/`、`~/.codex/agents/`：默认执行，不额外审批
   - 本地系统级动作（如 `~/Library/LaunchAgents`、shell profile、`launchctl`、`/Applications`、`brew services`）：先审批
   - 高风险远程或不可逆动作（如 `git push`、`ssh`、deploy、release）：先审批

## 热记忆与写回

1. `resume` 是热记忆层，不是长期记忆主库。
2. 自动续接条件：
   - 已绑定项目
   - 同项目
   - 最近 24 小时内有最近会话
3. 会话结束后的写回目标：
   - `01_working/`
   - `NEXT_ACTIONS.md`
   - 项目摘要页
   - `02_episodic/daily/YYYY-MM-DD.md`
4. 专题会话写回顺序：
   - 先写专题板
   - 再回卷一级项目板
   - 再刷新 `NEXT_ACTIONS.md`
   - 再触发一次检索增量同步
   - 最后刷新 `07_dashboards/`

## 自动化入口

1. 标准入口是 `ops/start-codex`。
2. 该入口会在启动前执行新项目发现。
3. 该入口会在会话结束后自动更新：
   - `runtime/project-bindings.json`
   - `runtime/session-router.json`
   - `01_working/NOW.md`
   - `NEXT_ACTIONS.md`
   - 项目摘要页
   - 当日日志
   - `runtime/retrieval/index.sqlite` 和 `runtime/retrieval/state.json` 的增量索引状态
4. app 直开模式下，后台 watcher `ops/codex_session_watcher.py` 会监听 `~/.codex/sessions` 并在 `task_complete` 后自动同步：
   - `runtime/project-bindings.json`
   - `runtime/session-router.json`
   - `01_working/NOW.md`
   - `NEXT_ACTIONS.md`
   - 项目摘要页
   - 当日日志
   - 检索层增量索引
5. 工作区总看板自动同步由 `ops/codex_dashboard_sync.py` 负责：
   - 消费 `runtime/events.ndjson`
   - 更新 `07_dashboards/HOME.md`
   - 更新 `07_dashboards/PROJECTS.md`
   - 更新 `07_dashboards/ACTIONS.md`
   - 更新 `07_dashboards/MEMORY_HEALTH.md`
6. 与这套系统有关的标准工作流统一收敛到 repo skill `obsidian-memory-workflow`。
7. 自 `v1.0.1` 起，根层还新增：
   - `/.codex/config.toml`
   - `/.codex/rules/generated.rules`
   它们只承载 Codex 运行默认和命令前缀规则投影，不承载任务事实源和业务控制真源。

## 报告产出

1. 面向管理者、协作者或外部分享的汇总报告统一放在：
   - `reports/`
2. 报告是对系统的可读汇总，不是机器事实源；任务与状态仍以 `memory/` 结构化主表为准。

## 分层事实源口径

当前固定链路：

`专题板写回 -> 一级项目板回卷 -> NEXT_ACTIONS 自动汇总 -> 07_dashboards 展示刷新`

当前机器事实源：

1. 专题板：`01_working/<项目名>-<专题>-跟进板.md`
2. 一级项目板：`01_working/<项目名>-项目板.md`
3. `NEXT_ACTIONS.md`
4. `runtime/session-router.json`
5. `runtime/project-bindings.json`

当前不作为任务事实源：

1. 项目摘要页 frontmatter
2. 项目摘要页正文
3. `07_dashboards/` 页面

补充约束：

1. 项目摘要页 `summary` 由人维护，不再承载当前任务状态。
2. 最近一次会话沉淀写入 `last_writeback_*` 字段和 `## 自动写回` 区块。
3. 专题板必须先回卷到一级项目板，再进入全局动作板。
4. `07_dashboards/` 只保留规则说明和自动区，不再与自动区并存手写状态摘要。

## 自动化呈报取证规则

1. 自动化呈报的任务与状态真源，只认：
   - 专题板
   - 一级项目板
   - `NEXT_ACTIONS.md`
2. `07_dashboards/` 和 `reports/` 是派生阅读层，不是任务真源。
3. `reports/ops/workspace-hub-health/latest.md` 只是“最近一次定时健康检查”，不是“当前时刻系统状态”的唯一真源。
4. 自动化如果要汇报“当前系统健康状态”，必须先做实时只读核验：
   - `python3 ops/codex_dashboard_sync.py verify-consistency`
   - `python3 ops/workspace_hub_route_check.py`
5. 若实时核验与 `latest.md` 冲突：
   - 以实时核验结果为准
   - 把 `latest.md` 中的相关问题描述为“历史告警”或“待确认关闭”
6. 若项目板 / `NEXT_ACTIONS.md` 与 `07_dashboards/` 冲突：
   - 以项目板与 `NEXT_ACTIONS.md` 为准
   - 把 dashboard 视为展示层滞后
7. 自动化不得把“总板空白”写成结论，除非项目板与 `NEXT_ACTIONS.md` 同时都没有有效 `todo / doing / blocked` 条目。
