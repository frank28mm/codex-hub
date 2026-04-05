# Codex Hub Workspace

这是 `Codex Hub` 的工作层。  
它负责运行这套系统的核心能力：

- 统一启动 `Codex`
- 读取和写回 `Obsidian` 记忆库
- 通过 `Feishu` 做远程协作
- 运行仓库内自包含的 Feishu bridge runtime
- 可选接入微信私聊版默认助手昵称 `CoCo`
- 通过 `Electron` 提供本地控制台
- 通过自动化脚本维护 dashboard、watcher、只读 Bitable 看板

默认机器人昵称是 `CoCo`。如果你想换成自己的名字，可以通过环境变量 `WORKSPACE_HUB_ASSISTANT_NAME` 自定义；本文里出现的 `CoCo` 都应理解为“默认昵称示例”，不是必须写死的产品名。

如果你把整个仓库当成一个产品来看：

- `workspace/` 是“代码和运行层”
- 仓库根层 `memory/` 是“模板化的长期记忆层”
- 本地实际运行时默认写入 sibling `memory.local/`

## 这是什么工具

`Codex Hub` 不是普通脚手架。它是一套本地优先的工作系统，适合：

1. 用 `Codex` 处理项目任务
2. 用 `Obsidian` 保存长期项目记忆和任务真相
3. 用 `Feishu` 做远程协作入口
4. 用微信私聊做轻量远程入口
5. 在手机端通过飞书只读看板查看项目和任务

## 公开版当前已经包含什么

这份公开版当前已经包含可运行的主链能力。  
也就是说，用户不需要自己再拼一套系统，而是可以在完成自己的账号、授权和资源配置后，按这份 README 的流程直接部署并使用：

- [ops/start-codex](./ops/start-codex)
  - 统一启动、项目路由和写回主链
- [AGENTS.md](./AGENTS.md)
  - 公开版完整运行协议
- [MEMORY_SYSTEM.md](./MEMORY_SYSTEM.md)
  - 公开版记忆系统协议
- [ops/feishu_agent.py](./ops/feishu_agent.py)
  - Feishu 对象操作与 OAuth
- [ops/feishu_projection.py](./ops/feishu_projection.py)
- [ops/workspace_job_schema.py](./ops/workspace_job_schema.py)
  - `Program Harness + Wake Loop` 的结构化 contract
- [ops/board_job_projector.py](./ops/board_job_projector.py)
  - 把项目板任务投影成可长期推进的 program/job
- [ops/background_job_executor.py](./ops/background_job_executor.py)
  - 长任务执行 loop、阶段推进、gate 与外发写回
- [ops/workspace_wake_broker.py](./ops/workspace_wake_broker.py)
  - wake 入口、项目级唤醒与恢复调度
- [ops/feishu_projection.py](./ops/feishu_projection.py)
  - 只读 Bitable 投影
- [ops/weixin_bridge.py](./ops/weixin_bridge.py)
  - 微信私聊 bridge、二维码登录与常驻轮询
- [bridge/feishu](./bridge/feishu)
  - 仓库内自包含的 Feishu 消息层、审批卡、回复卡与线程控制
- [bridge/feishu_long_connection_service.js](./bridge/feishu_long_connection_service.js)
  - Feishu 长连接 bridge 入口
- [ops/gstack_phase1_entry.py](./ops/gstack_phase1_entry.py)
  - gstack 主链与第二意见入口
- [ops/claude_code_runner.py](./ops/claude_code_runner.py)
  - Claude second-opinion 执行器
- [ops/bootstrap_workspace_hub.py](./ops/bootstrap_workspace_hub.py)
  - 一键初始化
- [ops/accept_product.py](./ops/accept_product.py)
  - 一键验收
- [ops/knowledge_intake.py](./ops/knowledge_intake.py)
  - Knowledge Base intake, topic routing, source registry, and clipper-driven ingestion

## 这些新增能力在使用上意味着什么

这轮公开版新增的，不只是几个文件，而是几条用户可感知的能力链。

### 1. Feishu 远程协作更完整了

现在公开版已经把 Feishu bridge runtime 直接放进仓库里，所以用户部署完成后，Feishu 这条线更接近“开箱即可接入”：

- 能在 Feishu 里继续项目线程
- 能接收结果卡和摘要
- 能处理批准动作
- 能把长结果通过更清楚的方式呈现出来

### 2. 公开版已经更接近真实 Codex Hub 工作方式

这次同时补进了：

- 更完整的运行协议
- `gstack` 主链
- second-opinion 执行器
- `Program Harness + Wake Loop`

所以公开版不只是“能启动 Codex”，而是更接近：

- 用 `Codex Hub` 统一调度项目
- 按协议读写记忆
- 用项目级 scope 持续推进长任务，而不是只停留在单轮对话
- 在复杂任务里走更完整的工作流
- 在需要时给出第二意见

### 3. 对用户来说，体验变化主要在这几个地方

部署完成后，最直接能感知到的是：

- Feishu 不再只是一个消息入口，而是一个真正的远程工作入口
- 长结果和复杂结果不再像工程日志，而更像产品化输出
- 公开版更容易接近你现在私有工作区里的实际使用方式

## 典型使用场景

### 场景 1：个人项目工作台

你在 `workspace/` 下直接使用 `Codex`，让它持续处理某个项目。
系统会自动结合运行时记忆根中的项目事实和上下文，而不是每次都从零开始。

### 场景 2：手机上的远程协作入口

你在 Feishu 里对自己创建的机器人发一句话：

- 创建任务
- 安排日程
- 新建文档
- 新建或更新多维表格
- 继续某个项目工作

这时 Feishu 只是入口，底层仍然是同一套 `Codex Hub`。公开版当前也已经把官方 `Feishu CLI / lark-cli` 接入到底层 transport 与对象 backend，尽量复现私有版里“默认助手昵称继续像现在一样用”的体验。

公开版当前已经进一步包含：

- `GFlow / workflow-aware` 入口推荐与运行摘要
- `Feishu callback + bridge recovery + execution lease`
- 受审批保护的 `OpenCLI` 执行面
- `health wake / catch-up` 自愈链

### 场景 3：长期记忆 + 可视化看板

项目事实长期保存在 sibling `memory.local/` 中；
同时又会自动投影到飞书多维表格，方便你在手机端查看项目总览和当前任务。

### 场景 4：桌面控制台

如果你需要本地线程视图、上下文抽屉或服务控制面，可以打开 Electron。
但这不是唯一入口，只是桌面工作台。

### 场景 5：把复杂任务交给系统持续推进

公开版现在已经包含 `Program Harness + Wake Loop`。
对人来说，不需要手工操作 loop，而是直接给出：

- 项目范围
- 目标
- 边界条件

例如：

- “只在 `TINT` 项目里推进 landing page 改版，先做到可验收。”
- “继续整理 `Codex Hub` 的 Feishu 迁移，并把结果同步回项目文档。”

系统会把它变成：

- 项目级 program
- 结构化 handoff bundle
- 定期 wake
- 每轮只推进一个子目标
- 自动写回项目板、报告和远程协作入口

这也是公开版现在和“普通聊天机器人”最大的区别之一。

如果你从旗舰版视角来理解，公开版现在的边界可以收成一句话：

- 保留默认助手昵称入口语义，并允许部署者改名
- 保留 `Codex Hub Core`
- 公开掉足够多的 workflow / wake / bridge / execution 机制
- 同时去掉个人项目、个人路径和敏感配置

## 依赖与官方链接

### 必需依赖

- `Python 3`
- `Node.js`
- `Codex CLI`
- macOS
  - 当前正式支持平台是 macOS
  - 后台自动任务依赖 `launchd`

### 可选但强烈建议

- `Obsidian`
  - 不是硬依赖，但用于查看和维护记忆库、使用 `obsidian://` 深链、获得完整体验
- `Feishu` 开放平台应用
  - 用于聊天协作、对象操作和只读 Bitable 看板
- `Electron`
  - 用于桌面控制台

### 依赖分层

为了让新手更容易判断“哪些现在就要补，哪些按需再装”，公开版当前把依赖拆成 4 层：

1. 核心必需
   - `python3`
   - `node`
   - `npm`
   - `npx`
   - `codex`
   - 已完成一次 `codex login`
2. 可自动安装
   - `requirements.txt` 里的 Python 包
   - `lark-cli` 与官方 `lark-*` skills（仅在启用 Feishu 时）
3. 推荐安装
   - `Codex.app`
   - `Obsidian.app`
4. 特性依赖
   - `tesseract / ocrmypdf / pdftoppm`
     - 只在启用 `Knowledge Base` 的 PDF/OCR intake 时需要
   - `Google Chrome`
     - 只在启用 `OpenCLI` 浏览器执行面时需要

### 官方链接

- `Codex`：[OpenAI Codex](https://developers.openai.com/codex/)
- `Feishu` 开放平台：[Feishu Open Platform](https://open.feishu.cn/)
- `Feishu` 产品官网：[飞书](https://www.feishu.cn/)
- `Obsidian`：[Obsidian](https://obsidian.md/)

## 目录说明

- `ops/`
  - 启动器、broker、watcher、dashboard sync、Feishu 工具层、只读投影等脚本
- `bridge/`
  - Feishu 长连接 bridge runtime、消息卡片、线程控制与 Node 回归测试
- `control/`
  - 控制真源，包括站点配置、模型默认值、飞书资源模板
- `apps/`
  - Electron 桌面工作台
- `.agents/`
  - Codex repo skills
- `.codex/`
  - 运行时生成的本地 Codex 配置
- `tests/`
  - 回归和 contract tests
- `reports/system/`
  - 产品说明、验收、路线图

## 普通用户怎么部署

下面是推荐顺序。

> [!IMPORTANT]
> **使用 Codex 来自行部署这套系统，效果最佳。**
> 这套产品本身就是围绕 `Codex` 构建的，所以最推荐的做法是：
> 让 `Codex` 在本地接手初始化、验收、Feishu 配置与桥接安装；
> 人类用户只负责少量必要授权。

> [!TIP]
> 如果你已经完成基础初始化，并且想把**微信私聊版 `CoCo`** 接进来，最推荐的方式也是直接让 `Codex` 来接。
>
> 你可以直接说：
>
> `帮我接微信私聊入口，启用微信私聊版 CoCo。请完成二维码登录、等待我扫码、安装后台常驻并验证状态。`
>
> 正常情况下，`Codex` 会替你：
> - 运行 `weixin_bridge` 的二维码登录
> - 把本地二维码展示给你扫码
> - 等你确认登录
> - 安装 LaunchAgent 常驻轮询
> - 最后检查 bridge 状态
>
> 对人类用户来说，通常只需要：
> **扫一下二维码。**
>
> 如果你想让 `Codex` 直接帮你把公开版激活起来，当前最短就是两句 prompt：
>
> 1. `请按 AGENTS 和 README 帮我完成这套 Codex Hub 的本地初始化。先检查依赖和目录，再执行 setup 和 acceptance。如果需要我确认，再明确告诉我。`
> 2. `帮我接微信私聊入口，启用微信私聊版 CoCo。请完成二维码登录、等待我扫码、安装后台常驻并验证状态。`
>
> 正常情况下，你只需要：
> - 先完成一次 `codex login`
> - 然后在第二句里扫一下二维码
>
> 如果你暂时先不走 Feishu，只想把公开版**快速手工跑起来**，当前最短路径已经收成两步：
>
> 1. `codex login`
> 2. `cd codex-hub/workspace && python3 ops/bootstrap_workspace_hub.py setup --install-launchagents`
>
> 这条路径只负责把**本地运行层**收好：
> Python 依赖、bootstrap、后台任务和 acceptance。
> 如果这台机器还没有 `lark-cli`，你也可以先把工具一起装上：
>
> `python3 ops/bootstrap_workspace_hub.py setup --install-launchagents --install-feishu-cli`
>
> 但这一步仍然只是装工具，不会替你完成 Feishu 配置。后面还要继续走专门的：
>
> `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app`

> [!TIP]
> `Codex` 只要从当前 `workspace/` 启动，就会自动读取：
> - [AGENTS.md](./AGENTS.md)
> - [MEMORY_SYSTEM.md](./MEMORY_SYSTEM.md)
>
> 这两份是运行协议，不需要用户手工复制到其他位置。

### 1. 克隆仓库

把整个仓库克隆到本地，例如：

```bash
git clone https://github.com/frank28mm/codex-hub.git
cd codex-hub/workspace
```

如果你不想先记命令，也可以从仓库根目录直接双击：

- `Install Codex Hub.command`
- `Validate Codex Hub.command`

### 2. 检查站点配置

打开：

- [control/site.yaml](./control/site.yaml)

默认值已经尽量做成通用模式：

- `workspace_root: auto`
- `memory_root: auto`

这意味着默认会使用：

- 当前 `workspace/`
- 旁边同级的 `memory.local/`

仓库根层的 `memory/` 仍然保留为模板，只在首次 bootstrap 时用来生成本机运行时记忆区。

如果你不想改目录结构，通常不需要改这两个值。

### 3. 执行一键 setup

```bash
python3 ops/bootstrap_workspace_hub.py setup --install-launchagents
```

这个 `setup` 会自动完成：

- 安装 `requirements.txt` 里的 Python 依赖
- 生成本地 `.codex/config.toml`
- 建立 `runtime/`、`logs/`、`reports/ops/`
- 确保 sibling `memory.local/` 运行时骨架存在（首次会由仓库根层 `memory/` 模板自动生成）
- bootstrap `Knowledge Base` project structure and launch its intake registry
- 执行：
  - `refresh-index`
  - `rebuild-all`
  - `verify-consistency`
- 安装：
  - watcher
  - dashboard sync
  - health check
  - Feishu projection
- 最后自动跑：
  - `python3 ops/accept_product.py run`

同时，`setup` 结束后写出的 `runtime/bootstrap-status.json` 现在会明确显示：

- `Codex CLI` 是否已经登录
- 哪些 Python 依赖已经装好
- 哪些工具属于推荐安装项
- 哪些只是特性依赖，不会阻塞基础使用

如果你想把 Python 依赖安装和 bootstrap 拆开，也可以先运行：

```bash
python3 ops/bootstrap_workspace_hub.py install-python-deps
```

然后再使用旧的初始化命令：

```bash
python3 ops/bootstrap_workspace_hub.py init
```

单独的 `init` 现在更适合高级或调试场景。它会：

- 生成本地 `.codex/config.toml`
- 建立 `runtime/`、`logs/`、`reports/ops/`
- 确保 sibling `memory.local/` 运行时骨架存在
- 执行：
  - `refresh-index`
  - `rebuild-all`
  - `verify-consistency`
- 输出：
  - `runtime/bootstrap-status.json`

如果你在高级模式下想把后台自动任务也一起装上，再运行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-launchagents
```

如果你已经准备好启用 Feishu 聊天入口，还可以继续执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge
```

### 4. 执行验收（如果你没有走 setup）

```bash
python3 ops/accept_product.py run
```

验收会检查：

- 路径是否完整
- `python3 / node / npm / npx / codex` 是否可用
- `Codex CLI` 是否已经完成登录
- `PyYAML / python-docx / openpyxl / pypdf / qrcode / certifi / requests / cryptography / openai` 是否已经装好
- 是否还残留个人现网路径
- bootstrap 是否完成
- 如果启用了 `Feishu`，还会检查 `lark-cli` 和本地配置是否存在

验收也会额外报告但默认不阻塞基础 PASS 的项目：

- `Codex.app / Obsidian.app / Google Chrome.app`
- `tesseract / ocrmypdf / pdftoppm`

也就是说，公开版现在会明确区分：

- “这台机器现在还不能正常使用”
- “这台机器能正常使用，但某些增强特性还没装”

结果会写到：

- `reports/system/product-acceptance-latest.md`

### 5. 完成最少人工授权

这套产品不是零人工，但人工步骤已经压到最少。

推荐理解方式是：

- 技术执行尽量交给 `Codex`
- 人类只做必须的人类动作：
  - `codex login`
  - Feishu 开放平台审核
  - Feishu OAuth 授权确认

#### Codex

如果本机还没有登录：

```bash
codex login
```

#### Obsidian

`Obsidian` 不是硬依赖。  
系统可以直接读写运行时记忆根文件；你只是在需要人类查看、深链跳转和长期浏览时再打开 `Obsidian`。

#### Feishu

如果你暂时不需要 Feishu，可以跳过这一段，先直接使用本地版。

如果你需要 Feishu 协作，请继续：

1. 运行：

```bash
python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app
```

这一步会尽量替你把 Feishu 接入收成一条链：

- 安装官方 `lark-cli`
- 安装官方 `lark-*` skills
- 拉起 Feishu 应用创建/配置
- 自动把 [control/site.yaml](./control/site.yaml) 切到 `feishu_enabled: true`
- 自动同步公开版运行时需要的 `app_id`
- 自动执行公开版统一的 Feishu 登录链
- 最后输出当前 `object_ops_ready / coco_bridge_ready / full_ready` 状态
- 公开版不再把系统钥匙串验证当成默认登录前置步骤

2. 打开 [control/feishu_resources.yaml](./control/feishu_resources.yaml)
3. 填入你的：
   - `owner_open_id`
   - 默认 `calendar_id`
   - 文档目录
   - 表格别名
   - 只读投影资源
4. 如果 `setup-feishu-cli` 最后没有达到 `full_ready=true`，再执行排查：

```bash
python3 ops/feishu_agent.py auth status
```

当前口径是：

- `object_ops_ready=true`：官方 CLI 对象能力已经可用
- `coco_bridge_ready=true`：`CoCo` bridge 凭据也已经齐
- `full_ready=true`：Feishu 完整可用

也就是说，只有 `full_ready=true` 才算这条 Feishu 接入真正做完。

只有在你明确要排查原生 `lark-cli` 身份时，才再额外执行：

```bash
lark-cli auth login --domain event,im,docs,drive,base,task,calendar,vc,minutes,contact,wiki,sheets,mail
lark-cli doctor
```

5. 确保你的 Feishu 应用 scope 已经通过审核并发布
6. 然后执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge
```

对普通用户来说，最省事的方式不是手工自己逐项配，而是：

- 让 `Codex` 来帮你完成飞书机器人接入、资源模板填写、统一登录链、bridge 安装和验证
- 你自己只在飞书页面完成必要确认

## Feishu 最简接入方式

如果你想要的能力只有：

- 在 Feishu 里聊天
- 让 Codex 操作飞书对象
- 在飞书里看只读项目/任务看板

那么当前仓库采用的就是**最简便的可工作方案**：

1. **一个你自己创建的 Feishu 应用**
2. **一条官方 `lark-cli` 配置与登录链**
3. **仓库内自包含的 Feishu bridge runtime**
4. **一个可选的只读 Bitable 投影**

也就是说，没有额外的 sidecar 编排层，没有第二套数据库，也没有独立的 web 管理后台。

它不是“绝对零配置”的最简单方案，因为 Feishu 平台权限审核本身就需要人工处理；但在保留完整能力的前提下，这已经是当前最小、最稳的一条线。

### 如何启动 Feishu 协作

当前推荐方式仍然是通过 Electron 宿主运行 Feishu 长连接桥接，因为这是公开版最顺的桌面路径；但 bridge runtime 本身已经直接包含在 `workspace/bridge/` 里，不再依赖私有仓外部代码。

先进入：

```bash
cd apps/electron-console
npm install
```

然后常用命令是：

```bash
npm run bridge:install
npm run bridge:status
```

如果你要单独验证 bridge 层本身，也可以进入：

```bash
cd bridge
npm install
npm test
```

如果你只是想先本地看 Electron 工作台，也可以：

```bash
npm run workspace
```

## 微信私聊入口（可选）

如果你希望再增加一个轻量远程入口，而不是只使用 Feishu，也可以启用微信私聊版 `CoCo`。

当前范围是：

- 只支持微信私聊
- 不支持微信群聊
- 继续走同一条 `Codex Hub` 主链

最推荐方式不是手工逐条执行，而是直接让 `Codex` 接手这条安装链。

你可以在 `workspace/` 里直接说：

`帮我接微信私聊入口，启用微信私聊版 CoCo。请完成二维码登录、等待我扫码、安装后台常驻并验证状态。`

然后：

- `Codex` 会启动二维码登录
- 给你展示本地二维码
- 等你扫码
- 自动安装 `weixin_bridge` 的 LaunchAgent
- 最后检查 `status`

启用后，你就可以通过微信私聊把任务送入同一套 `broker -> start-codex -> memory writeback` 主链。

如果你需要一条手工命令来完成同一件事，当前最短路径是：

```bash
python3 ops/weixin_bridge.py enable
```

这个命令会：

- 生成并打开本地二维码
- 等待你扫码登录
- 安装微信 bridge 的 LaunchAgent
- 最后输出当前 bridge 状态

## 部署等级

### A. 本地单机版

适合先把系统跑起来。

能力：

- `start-codex`
- memory
- watcher
- dashboard

不需要：

- Feishu
- Electron

### B. Feishu 协作版

增加：

- 你自己命名的 Feishu 机器人对话
- Feishu 对象操作
- 远程项目协作

需要：

- 一个 Feishu 应用
- 一次 OAuth
- 通过审核的 scope

### C. Bitable 看板版

在 Feishu 协作版基础上再增加：

- 项目总览只读表
- 当前任务只读表
- 手机端可视化看板

### D. 微信私聊版

在本地版基础上再增加：

- 微信私聊入口
- 常驻轮询
- 工作区级 `CoCo 私聊`

## 日常使用方式

最推荐的工作方式是：

1. 平时直接在 `workspace/` 下使用 `Codex`
2. 需要记忆时让系统自动读写 `memory.local/`
3. 需要远程协作时用 Feishu 找你自己创建的机器人
4. 需要轻量远程入口时用微信私聊找 `CoCo`
5. 需要看项目和任务可视化时看 Feishu Bitable
6. 需要本地控制台时打开 Electron

如果你是第一次上手，建议按这个顺序体验：

1. 先只跑本地版，确认 `bootstrap + acceptance` 都通过
2. 再打开 `Obsidian` 看 `memory.local/` 结构
3. 最后再接入 Feishu 和只读 Bitable，看远程协作体验

## 常用命令

### 初始化与验收

```bash
python3 ops/bootstrap_workspace_hub.py setup --install-launchagents
python3 ops/bootstrap_workspace_hub.py install-python-deps
python3 ops/bootstrap_workspace_hub.py install-feishu-cli
python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app
python3 ops/bootstrap_workspace_hub.py doctor-feature --feature knowledge-base
python3 ops/bootstrap_workspace_hub.py install-system-deps --group knowledge_base_pdf_ocr --dry-run
python3 ops/bootstrap_workspace_hub.py install-feature --feature electron --dry-run
python3 ops/bootstrap_workspace_hub.py init
python3 ops/bootstrap_workspace_hub.py status
python3 ops/accept_product.py run
```

如果某个具体能力还没 ready，不需要重新跑整套 setup。可以先用下面三类命令定点排查：

- `doctor-feature`：判断单个能力是否 ready，并给出下一步安装动作
- `install-system-deps`：按当前系统包管理器安装某组系统依赖，支持 `--dry-run`
- `install-feature`：只安装某个 feature 的主前置，比如 `electron / weixin / knowledge-base / opencli`

### 记忆与看板

```bash
python3 ops/codex_memory.py refresh-index
python3 ops/codex_dashboard_sync.py rebuild-all
python3 ops/codex_dashboard_sync.py verify-consistency
```

### Feishu

```bash
python3 ops/feishu_agent.py auth status
python3 ops/feishu_agent.py auth login
python3 ops/feishu_projection.py status
```

### Bridge

```bash
cd bridge
npm install
npm test
```

### Weixin

```bash
python3 ops/weixin_bridge.py enable
python3 ops/weixin_bridge.py login
python3 ops/weixin_bridge.py contract
python3 ops/weixin_bridge.py status
python3 ops/weixin_bridge.py install-launchagent
```

### Electron

```bash
cd apps/electron-console
npm install
npm run workspace
npm run bridge:status
```

## 关键文件

- [control/site.yaml](./control/site.yaml)
  - 站点级部署配置
- [control/feishu_resources.yaml](./control/feishu_resources.yaml)
  - Feishu 资源模板与投影目标
- [ops/bootstrap_workspace_hub.py](./ops/bootstrap_workspace_hub.py)
  - 一键初始化
- [ops/accept_product.py](./ops/accept_product.py)
  - 一键验收
- [ops/start-codex](./ops/start-codex)
  - 统一强启动入口
- [bridge/feishu/service.js](./bridge/feishu/service.js)
  - Feishu 线程、审批卡、长回复摘要卡与 Doc 镜像的核心消息层
- [bridge/feishu/outbound.js](./bridge/feishu/outbound.js)
  - Feishu 回复卡片与消息格式化
- [bridge/feishu_long_connection_service.js](./bridge/feishu_long_connection_service.js)
  - 长连接 bridge 入口
- [ops/weixin_bridge.py](./ops/weixin_bridge.py)
  - 微信私聊 bridge、二维码登录、daemon 与 LaunchAgent 安装

## 当前边界

- 默认不带任何真实飞书资源和 token
- 默认不带真实长期记忆
- 默认不带真实微信登录 token
- Feishu 权限审核和第一次 OAuth 无法完全自动化，只能做到“少量人工授权后长期自动续期”
- 微信桥当前只支持私聊，不支持群聊
- 当前正式支持平台是 macOS；Windows 尚未完成后台任务、常驻 bridge、通知与保活适配
