# Electron 控制台

`apps/electron-console` 是 `v1.0.6` 集成发布的人类操作工作台，承载 Electron shell 与 Next.js 渲染器。

默认机器人昵称是 `CoCo`。如果部署者希望改成别的名字，可以设置环境变量 `WORKSPACE_HUB_ASSISTANT_NAME`；本页出现的 `CoCo` 都只是默认昵称示例。

## 产品定位

- 这个控制台既是 Codex 的桌面主入口，也是 Feishu 桥接的桌面宿主。
- 目标是把当前桌面能力收成 3 个清晰主视图，并把 Feishu 线程直接并入 `Codex 交互`：
  - `项目看板`：所有工作区项目的总览、状态和 drill-down 入口
  - `Codex 交互`：桌面发起任务、续接会话、打开原生 Codex App 的主入口
  - `系统与设置`：桥接服务、健康、快捷启动、设置和运行说明
- 原生 Codex App 仍保留为兜底入口，但 Electron 是默认的桌面前端。
- Codex CLI 仍然是执行引擎，Electron 只负责 UI、上下文入口和 Feishu 协作可视化。

## 关键集成契约

- 共享 broker 由产品工作区的 `ops/local_broker.py` 持有，Electron 通过 IPC 读取：overview、projects、review、coordination、health、bridge-status、bridge-settings、bridge-conversations。
- 高风险动作走 `/approve` 流；普通命令直接通过 `codex exec`/`codex resume`。
- Electron 的 `Codex 交互` 不再直接拼 `codex exec/resume`，而是统一通过产品工作区的 `ops/start-codex` 拉起 Codex，会继承项目自动发现、`codex_context.py suggest`、会后写回和 dashboard 上卷链路。
- Electron 的 `Codex 交互` 现在支持两档执行权限：
  - `默认权限`：沿用工作区默认档（`workspace-write + on-request + network_access=true`）
  - `完全访问`：通过 `electron-full-access` 执行档拉起 `start-codex`，等价于 `danger-full-access`
- 默认助手昵称对应的本地守护由 LaunchAgent 管理，Electron 通过 `bridge-host` 读取运行态和授权状态。
- `bridge-status` 聚合的 Feishu bridge status 数据会被 renderer 读出，用于状态卡和错误提示。

## 中文 Operator Workbench 说明

- 工作台左侧固定为中文导航，只保留 3 个主入口：`项目看板 / Codex 交互 / 系统与设置`。
- 左侧壳已收成 CodePilot 风格的窄图标导轨：导轨只承载视图切换，操作者信息与系统状态统一上移到主区顶栏。
- `项目看板` 负责所有工作区项目的统一呈现，并能直接跳进对应的 Codex 交互或飞书线程。
- `项目看板` 已进一步收成“项目列表 + 当前项目工作区视角”的双栏结构：左侧统一列出项目，右侧聚焦当前项目的状态、下一步、Feishu 线程、审核与协同摘要。
- `项目看板` 右侧工作区还细分成 `总览 / 审核 / 协同 / 飞书 / 健康` 五个项目内视图，避免把项目状态和系统运维混成一块。
- `Codex 交互` 负责替代单独使用 Codex App 的基础入口，按聊天视角保存“你发给 Codex 的任务”和“Codex 返回的结果”，并支持本地多会话切换与搜索。
- `Codex 交互` 主体是“左侧会话列表 + 中间聊天时间线 + 底部输入区”的前端结构，技术摘要默认折叠，保持聊天优先。
- `Codex 交互` 左侧现在统一收纳两类线程：`桌面对话` 和 `飞书来源线程`；右侧只保留一个主聊天工作区，不再为 Feishu 单独开平级页面。
- `Codex 交互` 本轮又把左侧进一步收成“统一线程栏”：桌面对话与飞书来源线程按最近活跃时间混排在同一列里，不再拆成两块独立列表。
- `Codex 交互` 现在把过滤进一步压成统一线程语义：`全部 / 待授权 / 运行中 / 失败 / 历史`，不再把桌面对话和飞书线程拆成两套筛选心智。
- `Codex 交互` 的对话区已切成聊天气泡时间线，默认就按“你 / Codex”还原，不再是工程卡片堆叠。
- `Codex 交互` 左侧线程栏也继续向 CodePilot 靠拢：顶部只保留状态胶囊、主按钮、搜索和一行统一筛选，线程卡片默认只显示来源、标题、最近请求和最近活跃时间。
- `Codex 交互` 左侧线程栏本轮继续向 CodePilot 的项目会话栏靠拢：统一线程会按项目名或来源身份分组，再在组内按最近活跃时间排列，阅读时更接近真正的会话导航，而不是一长列平铺卡片。
- `Codex 交互` 左侧线程栏本轮继续把线程卡从“状态卡”收成“会话项”：标题、来源、最近请求和最近活跃时间成为主信息，执行/授权状态降成次级标记，更接近 CodePilot 的会话列表阅读节奏。
- `Codex 交互` 的输入区现在也按聊天前端重做：默认直接输入任务内容，只在需要切动作、项目或 Session 时才展开“高级设置”，避免把桌面主入口做成工程表单。
- `Codex 交互` 的主聊天区本轮也补上了更接近 CodePilot 的轻量工具条：常用的权限切换、动作切换、新对话和重置输入都放进聊天区上方，而不是埋在工程化表单里。
- `Codex 交互` 的输入区本轮又继续按 CodePilot 收口：权限切换、动作切换、新对话、重置输入和打开原生 Codex App 现在都收进输入框下方的聊天工具条，主聊天区不再先摆一排控制项。
- `Codex 交互` 本轮又继续按 CodePilot 的极简外壳减噪：在 `Codex 交互` 视图里不再显示一整条全局大头信息，左侧统一线程栏继续收成“连接状态 + 新对话按钮 + 单个项目入口图标 + 搜索 + 统一筛选 + 会话列表”，右侧主聊天区则只保留聊天标题、轻量上下文入口、聊天时间线和底部输入区。
- `Codex 交互` 的主聊天区本轮也真正收成一套统一聊天壳：无论当前线程来自桌面对话还是飞书，顶部上下文、聊天时间线和“更多上下文”抽屉都走同一套结构，不再保留两套明显分裂的主区页面。
- `Codex 交互` 的高级设置现在已经补上 `访问权限` 选择器，可在 `默认权限` 和 `完全访问` 之间切换，并会记住上次选择。
- `Codex 交互` 现在默认先呈现聊天时间线，项目/状态/Session 这类技术摘要默认折叠到“查看会话详情”里，避免一打开就被运维字段打断。
- Feishu 线程不再作为单独主视图存在，而是直接作为 `Codex 交互` 左侧会话栏中的来源线程；点击后右侧直接还原聊天、执行、授权和汇报。
- Feishu 线程沿用原始私聊名或群名，并优先显示最近请求、最近汇报、授权状态和项目路由摘要。
- `系统与设置` 收纳服务、设置、健康和说明，避免把运维参数堆在主工作流里。
- `Codex 交互` 已经转向真正的聊天前端体验：左侧列表优先展示 `全部 / 工作区级 / 项目级 / 运行中 / 失败` 会话，右侧则是聊天气泡时间线、快捷操作、项目上下文与输入输出，彻底摆脱了早期卡片堆叠式的外观。
- `Codex 交互` 里的 Feishu 来源线程会直接还原手机端或群聊里看到的消息，并允许在桌面端一键跳转到当前项目或续接为桌面对话。
- `系统与设置 -> CoCo 服务` 已直接展示事件流停滞次数、最近停滞时间、累计重连次数、最近恢复耗时和持久化校验耗时，封板阶段主要用来盯 `event_stalled / reconnect` 可靠性。
- `系统与设置 -> CoCo 服务` 本轮又补了 `最近送达时间 / 最近送达阶段 / 确认等待中 / 确认等待秒数 / 确认静默次数 / 最近确认静默`，用来判断“已经 ack 但后续结果迟迟没送达”的窗口是否被压缩。
- `系统与设置 -> CoCo 服务` 现在还会在 bridge 重连后展示“恢复前比对线程数 / 恢复后线程数”。`ack_stalled` 仍会作为观察指标展示，但不再单独触发自动重连；真正恢复以“已连接 + 非 stale + 非 event stalled”为准。
- `系统与设置 -> CoCo 服务` 现在还会在自动恢复后补做一次线程审计，确认项目路由和 session 没有在恢复后再次漂移。
- `系统与设置 -> CoCo 服务` 本轮又补了正式健康摘要：服务页和 `coco-bridge-service.js status` 会直接输出 `health_summary_* / last_recovery_summary / last_persistence_summary / thread_snapshot_summary / latest_anomaly_summary`，用于一眼判断当前是否可继续信任 CoCo 线程。
- 全部静态文字已经切成中文，空态、错误态和操作提示都以操作员视角表达。

## 运行方式

```bash
cd /path/to/Codex\ Hub/workspace/apps/electron-console
npm install
npm run workspace
```

该命令会启动 Renderer 与 Electron shell，并自动连接 `CoCo` 服务。

开发阶段也可选用分开方式：

```bash
cd /path/to/Codex\ Hub/workspace/apps/electron-console
npm run renderer:dev
npm run dev
```

## macOS 图标快捷启动

Electron 提供了一个自动化脚本，可在 macOS 上生成图标式快捷入口。执行 `npm run launcher:install` 后，脚本会在 `~/Applications` 下创建 `Codex Hub 工作台.app`，该 App 通过 Terminal 启动 `npm run workspace`，并可直接拖到 Dock 或快捷启动栏。脚本会在完成后输出路径和提示，便于检验机体是否就绪。

也可以配合下面两个命令查看或移除该入口：

```bash
npm run launcher:status
npm run launcher:uninstall
```

如果你更偏好从工作台内部操作，也可以在 `系统与设置 -> 桌面快捷启动` 中直接安装、重装或移除。

Recent Command Results 会在控制台内缓存，便于快速浏览近几次执行。

## 验证命令

- `npm run renderer:build`：编译 Next.js 渲染器。
- `npm run smoke`：无界面 smoke 检查。
- `node coco-bridge-service.js verify-persistence`：重启 CoCo LaunchAgent 并校验线程路由与会话持久化。

## 目录说明

- `main.js`：Electron 主进程与 IPC 桥。
- `preload.js`：安全通道。
- `renderer/`：Next.js 渲染器。
- `renderer/app/page.js`：中文 Operator 视图。
- `globals.css`：工作台配色与空态样式。
