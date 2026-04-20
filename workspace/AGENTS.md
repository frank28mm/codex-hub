# Codex Hub Workspace AGENTS

1. 所有 Codex 会话默认从当前 `workspace/` 根目录启动。
2. 这里是行动系统工作层，不是长期记忆库；公开版运行时长期记忆默认位于同级目录 `../memory.local/`，仓库根层 `../memory/` 只保留模板骨架。
3. `projects/` 目录用于承载真实项目的本地工作副本，不属于行动系统仓的版本管理范围。
4. 不要把 `projects/` 下的项目文件、项目源码、项目构建产物提交到 `Codex Hub` 产品仓。
5. 新会话启动时只允许读取最小入口，不允许全量扫描整个运行时记忆根。
6. 先读取 [MEMORY_SYSTEM.md](./MEMORY_SYSTEM.md)。
7. `ops/start-codex` 是最强保证的显式入口：
   - 不要求用户先手动打开 Obsidian
   - 会在需要时自动确保运行时记忆根（默认 sibling `memory.local/`）可被后台读取，并在安装了 Obsidian 时尝试把 Vault 打开在后台
   - 启动前自动发现并注册新项目
   - 启动后自动维护 `session-router.json`
   - 在项目/专题绑定后自动输出上下文建议摘要
   - 会话结束后自动写回运行时记忆根
8. 如果 Codex app 直接在当前 `workspace/` 中打开新对话，这就是推荐的日常主模式，也必须遵循同一套协议，而不是退化成普通项目对话。
9. app 直开模式下，优先依赖后台守护进程 [ops/codex_session_watcher.py](./ops/codex_session_watcher.py) 自动同步本地 session 到运行时记忆根。
10. 与这套系统有关的运行时记忆读写、项目路由、会后写回，统一遵循 repo skill `obsidian-memory-workflow`。
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
17. 工作区总看板同步器是 [ops/codex_dashboard_sync.py](./ops/codex_dashboard_sync.py)：
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
   - 相关系统演进任务应落在运行时记忆根的 `Codex Hub-项目板`
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
   - Feishu 普通任务和飞书对象操作默认走 `workspace-write + network_access=true`
   - 写入 `~/.codex/skills/`、`~/.codex/agents/` 这类本地 Codex 扩展目录，默认走 `feishu-local-extend`，不需要额外审批
   - 写入 `~/Library/LaunchAgents`、shell profile、`/Applications`、`brew services` 这类本地系统级动作时，必须先审批
   - `git push`、`ssh`、deploy、release 等高风险远程或不可逆动作，继续保留审批
26. 这份 `workspace/AGENTS.md` 是运行协议，不需要用户手工替换到别处：
   - 只要 Codex 在当前 `workspace/` 下启动，它就会直接读取这里
   - 根目录的 `AGENTS.md` 只负责帮助部署这套产品，不替代运行协议
27. `gstack` 调研文档中的第一层通用 skills 已进入正式安装态：
   - 维护真源位于 `workspace/skills/`
   - 当前包含：`investigate`、`review`、`qa`、`guard`
   - 已安装到用户级 `~/.codex/skills/`
   - 仓库内副本负责继续迭代；`~/.codex/skills/` 是当前机器上的已安装副本
28. 当前基于 `gstack` 迁移的工作流框架，助手模式规则固定为：
   - 当前 `Phase 1 + Phase 2 + Phase 3` 已正式落地并安装，`Phase 4` 已正式落地到仓库真源：
     - `workspace/skills/office-hours`
     - `workspace/skills/plan-ceo-review`
     - `workspace/skills/plan-eng-review`
     - `workspace/skills/_shared/gstack_phase1_protocols.md`
     - `workspace/skills/browse`
     - `workspace/skills/document-release`
     - `workspace/skills/retro`
     - `workspace/skills/_shared/gstack_phase2_protocols.md`
     - `workspace/skills/ship`
     - `workspace/skills/careful`
     - `workspace/skills/freeze`
     - `workspace/skills/unfreeze`
     - `workspace/skills/_shared/gstack_phase3_protocols.md`
     - `workspace/skills/claude-review`
     - `workspace/skills/claude-challenge`
     - `workspace/skills/claude-consult`
     - `workspace/skills/_shared/gstack_phase4_protocols.md`
     - `workspace/ops/gstack_phase1_entry.py`
     - 当前用户级安装副本已稳定覆盖 `Phase 1 + Phase 2 + Phase 3`；`Phase 4` 继续以仓库真源作为当前维护入口，后续可再同步到 `~/.codex/skills/`
   - `Feishu / Electron / broker` 的职责固定为只拉起 `Codex` 主线程，不直接调用 `Claude Code`
   - 当自然语言命中第二意见层时，是否进入 `claude-review / claude-challenge / claude-consult` 由 `Codex` 主线程内部判断，再由 `Codex` 内部通过 `python3 ops/claude_code_runner.py` 调用 `Claude Code`
   - `~/.codex/skills/claude-*` 安装副本只作为本机原生发现增强项，不再作为 `Feishu / Electron` 路径或 `Phase 4` 可用性的前置条件
   - `superpowers` 当前允许作为编程任务的外部执行层接入，但定位固定为“编程方法层”，不是 `Codex Hub` 的系统真源：
     - 官方 clone 路径固定为：`~/.codex/superpowers`
     - 官方发现路径固定为：`~/.agents/skills/superpowers`
     - 只在编程实现、代码 review、测试验证、开发收口时调用；典型 skill 包括：`writing-plans`、`executing-plans`、`systematic-debugging`、`requesting-code-review`、`test-driven-development`、`verification-before-completion`、`finishing-a-development-branch`
     - `Codex Hub` 继续负责项目板、记忆、bridge、harness、writeback、跨平台调度；不要让 `superpowers` 接管这些系统层职责
     - 如果 `superpowers` 未安装或发现链断开，不要把它当作阻塞；应回退到 `Codex Hub` 自带的工作流与 skill
   - 当前 second-opinion 的正式执行 contract 固定为：由 `python3 ops/gstack_phase1_entry.py` 统一打包 `question / artifact / current_judgment / extra_context`，形成 `codex-hub.second-opinion.request.v1`；结果统一回收到 `codex-hub.second-opinion.response.v1` 的标准 `structured_output`，并同时产出可直接复用的 `main_thread_handoff`
   - 默认入口仍然是自然语言，不要求用户先知道 skill 名、工作流名或内部系统结构
   - 这条规则当前只适用于 `gstack -> Codex` 这条岗位型工作流框架，不泛化为系统内所有已有或未来 skills 的统一默认行为
   - 这套框架的识别模型不是只有三类，而是分阶段识别：
     - 入口层：`office-hours / plan-ceo-review / plan-eng-review`
     - 执行层：`investigate / review / qa / browse`
     - 交付层：`document-release / retro / ship`
     - 姿态层：`guard / careful / freeze / unfreeze`
     - 第二意见层：`claude-review / claude-challenge / claude-consult`
   - 文档分工必须明确：
     - `AGENTS.md` 现在就要写清完整五层模型，以及当前已经稳定、已经可执行的识别细则
     - 当前入口层、执行层、交付层、姿态层，以及第二意见层都已具备稳定的仓库真源、共享协议和最小契约测试，因此这些层的详细“识别条件 + 反例 + 交互示例”现在都应写进 `AGENTS.md`
   - 当用户的自然语言已经明确指向一个具体任务，而系统判断此时更适合进入这套工作流中的某一层或某一段路径时，Codex 应主动切到“助手/助理”式引导姿态
   - 识别条件必须具体，而不是模糊感觉：
     - 入口层：
       - 如果用户在“想法、需求、项目方向”上还很散，典型表述如“帮我梳理一下”“我想清楚再决定做不做”“这个想法怎么落地”，优先识别为 `office-hours`
       - 如果用户在问“值不值得做、方向对不对、优先级怎么排、产品上是否成立”，典型表述如“这个方向值不值得做”“从产品角度怎么看”，优先识别为 `plan-ceo-review`
       - 如果用户在问“技术上行不行、架构怎么拆、风险和测试怎么控”，典型表述如“技术上怎么做”“这个方案有哪些技术风险”，优先识别为 `plan-eng-review`
     - 如果一个问题同时命中上面两类或三类，应优先建议串联成 `office-hours -> plan-ceo-review -> plan-eng-review`
     - 执行层：
       - 如果重点是“现象不对、原因不明、失败了、卡住了”，优先识别为 `investigate`
       - 如果重点是“已有改动/方案/PR，需要找风险和缺口”，优先识别为 `review`
       - 如果重点是“实现已完成或接近完成，需要验证和验收”，优先识别为 `qa`
       - 如果重点是“真实页面、浏览器流程、UI 交互或前端路径需要 live evidence”，优先识别为 `browse`
       - 如果一个请求同时包含“先定位原因，再给现有改动找风险，再做验收”，允许建议跨层路径，例如 `investigate -> review -> qa`
       - 当前最小稳定实现由 `python3 ops/gstack_phase1_entry.py suggest --prompt '...'` 提供；它已经能稳定给出入口层、执行层、交付层与跨层组合的建议路径与初始行动方案
     - 交付层：
       - 如果重点是“要同步发布说明、更新文档、整理使用说明或变更说明”，优先识别为 `document-release`
       - 如果重点是“一个阶段已经做完，需要复盘、提炼经验和下一轮改进”，优先识别为 `retro`
       - 如果重点是“准备发版、交付、提交、handoff，想判断现在能不能正式推出去”，优先识别为 `ship`
       - 如果一个请求是“先审当前改动或验证，再判断是否能发”，允许建议 `review -> ship` 或 `qa -> ship`
     - 姿态层：
       - 如果重点是“任务还能继续，但风险或模糊性偏高，需要更窄、更显式的谨慎路径”，优先识别为 `careful`
       - 如果重点是“先冻结写操作、发布动作或某类高风险修改，保留只读排查和计划”，优先识别为 `freeze`
       - 如果重点是“重新检查 freeze gate 是否满足，并判断能否恢复修改或发布”，优先识别为 `unfreeze`
       - 如果一个请求同时命中执行/交付层与姿态层，允许建议组合路径，例如 `review -> ship -> careful` 或 `investigate -> freeze`
     - 第二意见层：
       - 如果重点是“已有具体方案、改动、判断或发布结论，想再要一轮独立复审或第二意见”，优先识别为 `claude-review`
       - 如果重点是“想让系统站在反方、给出最强反对意见、挑刺或做压力测试”，优先识别为 `claude-challenge`
       - 如果重点是“想要一个轻量顾问式补充视角，帮助重构 framing、tradeoff 或备选方案”，优先识别为 `claude-consult`
       - 如果一个请求同时命中执行/交付层与第二意见层，允许建议组合路径，例如 `review -> claude-review`、`ship -> claude-challenge` 或 `plan-eng-review -> claude-consult`
   - 反例也要明确：
     - 简单事实查询、单一步骤执行、小改动直接实现、用户已明确要求“直接做”的任务，不默认触发这套工作流框架
   - 当前已稳定的交互示例至少包括：
     - “帮我梳理一下这个想法” -> `office-hours`
     - “这个方向值不值得做” -> `plan-ceo-review`
     - “技术上怎么落地，有哪些风险” -> `plan-eng-review`
     - “这个 bug 为什么会这样，先帮我定位根因” -> `investigate`
     - “帮我审一下这次改动有没有问题” -> `review`
     - “已经改好了，帮我测一下并验收” -> `qa`
     - “帮我用真实浏览器看一下这个页面流程和按钮交互” -> `browse`
     - “帮我把这次变更同步成发布说明和更新文档” -> `document-release`
     - “这一轮做完了，帮我做个复盘” -> `retro`
     - “这个版本准备发版了，帮我判断是不是能发” -> `ship`
     - “这次变更风险很高，先谨慎一点推进” -> `careful`
     - “先冻结所有写操作，等我确认后再继续” -> `freeze`
     - “现在可以解除冻结继续推进了吗？” -> `unfreeze`
     - “这个方案请再给我一个 Claude 风格的第二意见” -> `claude-review`
     - “你站在反方挑战一下这个方案” -> `claude-challenge`
     - “请再给我一个顾问式建议，帮我看 tradeoff” -> `claude-consult`
   - 这种引导的目标不是教学，而是降低用户负担：用自然语言说明“我们可以这样更省力地做”，并同时给出推荐的初始行动方案
   - 除非用户明确要求，否则不要把这套工作流技能当成必须显式操作的菜单；应优先由 Codex 在内部识别并推荐最合适的路径
   - 只有在任务足够复杂、直接开做风险较高、或使用工作流会显著提高质量时，才主动提出这类引导，避免把简单问题也流程化
29. `AGENTS.md` 改写治理规则：
   - 这里只写会影响 Codex 当前行为的稳定协议和持久规则
   - 不写临时阻塞原因、沙箱历史、测试通过数、一次性迁移状态或“后续再做”的过程说明
   - 解释性背景、部署说明、验收结果和历史原因应写入 `README.md`、系统报告或项目记忆，不写进 `AGENTS.md`
30. 工具与依赖补齐规则固定为：
   - 如果一个方案成立的前提是本机缺少某个标准工具、OCR/渲染链、CLI 依赖或系统组件，不要因为“当前没有”就直接把方案降级成更弱版本
   - 应优先判断该依赖是否属于可安装、可验证、可自动化补齐的前置条件
   - 如果属于标准可补齐依赖，应把“安装 -> 配置 -> 验证”写入执行计划，并在权限允许时直接补齐
   - 只有在确实不可安装、需要用户外部账号/审核、或风险过高时，才把它明确列为阻塞并向用户说明
31. 开发后的验证与回归规则固定为：
   - 任何代码改动、自动化改动、运行链改动、配置机制改动，在宣布完成前都必须经过至少一轮验证，不允许只改代码不验证
   - 验证优先级固定为：
     1. 先跑与改动直接对应的单元测试/契约测试
     2. 再跑该链路的集成验证、smoke 或最小真实调用
     3. 如改动涉及看板、写回、路由或记忆系统，再补跑一致性检查
   - 如果当前仓库还没有覆盖该改动的测试，应优先补最小必要测试，而不是把“没有测试”当成默认状态
   - 如果验证失败，默认先进入 debug/定位流程，不要在已知失败状态下宣称完成
   - 如果因为外部条件限制无法完成某一类验证，必须明确说明缺了哪类验证、为什么没法跑、当前剩余风险是什么
   - 对用户可感知的能力，优先使用可重复自动化验证；能脚本化的验证，不默认依赖人工口头确认
