# Codex Hub

`Codex Hub` 是一套以 `Codex + Obsidian + Feishu` 为核心，并可选接入微信私聊入口的本地工作系统。

它解决的是：

- 在本地长期维护项目和任务真相
- 通过 `Codex` 执行项目级工作
- 通过 `Feishu` 做远程协作、日程/任务/文档/多维表格操作
- 可选用微信私聊把 `CoCo` 当成第二个远程入口
- 把结果自动写回记忆系统，并同步成手机可查看的只读看板

用户只要完成自己的 `Codex` 登录、`Feishu` 应用配置与必要授权，就可以按这套产品的推荐方式来部署，并复现 `Codex Hub` 的核心使用方式：

- 统一启动与项目路由
- `Obsidian-compatible` 记忆系统读写
- `GFlow / workflow-aware` 入口体验
- 长任务 `Program Harness + Wake Loop`
- 自包含的 Feishu bridge runtime、callback、bridge recovery 与 execution lease 主链
- 受审批保护的 `OpenCLI` 执行面
- health wake / catch-up 自愈链
- 基于官方 `Feishu CLI / lark-cli` 的对象操作主链
- 可选的微信私聊 bridge
- 只读 Bitable 投影
- 可选的 Electron 桌面宿主
- 一键初始化与一键验收

> [!NOTE]
> 这套系统使用 **Obsidian-compatible Vault** 作为长期记忆模型。  
> 它没有脱离 Obsidian 的记忆体系，但已经脱离了 `Obsidian` GUI 的硬依赖：
> 没装 `Obsidian` 时，核心的项目路由、写回和看板同步仍然可以通过直接读写 `memory/` 文件来工作。  
> 如果你想获得完整体验，仍然强烈建议安装 `Obsidian`。

这份仓库是 `Codex Hub` 的**可复制、可公开、可本地部署**版本
> [!IMPORTANT]
> **最推荐的部署方式：使用 Codex 自行完成部署，效果最佳。**
> 这套系统本来就是给 `Codex` 驱动的工作流准备的，所以最顺的方式不是人类手工一点点配，而是让 `Codex` 直接在本地接手：
> 检查依赖、执行 bootstrap、执行 acceptance、引导 Feishu 配置和 bridge 安装。  
> 对人类用户来说，通常只需要完成少量授权，例如：
> `codex login`、Feishu 开放平台权限审核、Feishu OAuth 登录确认。

> [!TIP]
> 如果你准备让 coding agent 直接帮你部署，请先看根目录的 [AGENTS.md](./AGENTS.md)。
> 这里专门写了“agent 应该怎么接手部署这套系统”的最优流程。

> [!TIP]
> 如果你已经完成了 `codex login`，并且想马上把**微信私聊入口**也接进来，最推荐的方式不是手工敲命令，而是直接让 `Codex` 来做。
>
> 你可以在 `workspace/` 里直接对 `Codex` 说：
>
> `帮我接微信私聊入口，启用微信私聊版 CoCo。请完成二维码登录、等待我扫码、安装后台常驻并验证状态。`
>
> 正常情况下，`Codex` 会替你：
> - 启动微信二维码登录
> - 把本地二维码展示出来让你扫码
> - 等你扫码确认
> - 安装 `weixin_bridge` 的后台常驻
> - 最后验证桥接状态
>
> 对人类来说，通常只需要做一件事：
> **用微信扫一扫本地弹出的二维码。**

> [!TIP]
> 如果你想让 `Codex` 直接帮你把公开版激活起来，当前最短就是两句 prompt：
>
> 1. `请按 AGENTS 和 README 帮我完成这套 Codex Hub 的本地初始化。先检查依赖和目录，再执行 setup 和 acceptance。如果需要我确认，再明确告诉我。`
> 2. `帮我接微信私聊入口，启用微信私聊版 CoCo。请完成二维码登录、等待我扫码、安装后台常驻并验证状态。`
>
> 正常情况下，你只需要：
> - 先完成一次 `codex login`
> - 然后在第二句里扫一下二维码

> [!TIP]
> 如果你暂时先不走 Feishu，只想把公开版**快速手工跑起来**，当前最短路径已经收成两步：
>
> 1. `codex login`
> 2. `cd codex-hub/workspace && python3 ops/bootstrap_workspace_hub.py setup --install-launchagents`
>
> 这条路径只负责把**本地版**收好：
> Python 依赖、bootstrap、后台任务和 acceptance。
> 如果你后面还要接 Feishu，不要停在 `--install-feishu-cli`，而是继续走专门的：
>
> `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app`

> [!TIP]
> 真正驱动系统运行的协议文件在：
> - [workspace/AGENTS.md](./workspace/AGENTS.md)
> - [workspace/MEMORY_SYSTEM.md](./workspace/MEMORY_SYSTEM.md)
>
> 只要 `Codex` 是从 `workspace/` 启动的，它就会自动读取这两份文件；用户不需要手工复制或替换它们。

## 当前正式支持

- **正式支持：macOS**
- 当前这版后台自动化依赖 `launchd / LaunchAgents`
- Windows 目前**无法支持**

## 仓库结构

- `workspace/`
  - 产品代码、运行协议、Feishu bridge runtime、启动器、Electron 前端、自动化脚本、测试和系统文档
- `memory/`
  - 模板化的长期记忆库骨架，供新部署者本地初始化使用

## 依赖与官方链接

### 核心产品

- `Codex`
  - 官方文档：[OpenAI Codex](https://developers.openai.com/codex/)
- `Feishu`
  - 开放平台：[Feishu Open Platform](https://open.feishu.cn/)
  - 产品官网：[飞书](https://www.feishu.cn/)
- `Obsidian`
  - 官网：[Obsidian](https://obsidian.md/)

### 本地依赖

- `Python 3`
- `Node.js`
- `Codex CLI`
- macOS

## 它适合谁

适合想要在本地搭建以下工作方式的人：

1. 用 `Codex` 驱动日常项目执行
2. 用 `Obsidian` 维护长期记忆和事实源
3. 用 `Feishu` 或微信私聊做远程协作入口
4. 用飞书多维表格看手机端只读项目看板

## 小白用户最常见的使用场景

如果你第一次接触这套系统，可以先把它理解成下面几种实际用法：

### 场景 1：把 Codex 变成你的长期项目助理

你平时直接在 `workspace/` 里和 `Codex` 对话，让它：

- 继续某个项目
- 读取项目上下文
- 更新项目板和专题板
- 做完后把结果写回 `memory/`

适合：

- 自己长期推进多个项目
- 不想每次重新解释项目背景

### 场景 2：不用坐在电脑前，也能在远程入口里交代事情

你在 Feishu 里对自己创建的机器人，或者在微信私聊里对 `CoCo` 说：

- 帮我安排一个日程
- 新建一个项目文档
- 创建一个飞书任务
- 建一个多维表格
- 继续处理某个项目

这时 `Codex Hub` 会把远程聊天入口当成工作入口，而不是第二套系统。公开版当前也默认保留 `CoCo` 入口语义：人类继续通过私聊 `CoCo` 或在项目群里 `@CoCo` 发起工作，底层则优先吃官方 `Feishu CLI / lark-cli` 的 transport 与对象能力。

适合：

- 在手机上远程给 `Codex` 派活
- 在开会、出门、通勤时继续推进工作

### 场景 3：把 Obsidian 当长期记忆库，而不是聊天记录堆

你不需要自己手工维护复杂数据库。
系统会把真正需要长期保留的项目信息写回 `memory/`，你可以用 `Obsidian` 查看：

- 当前有哪些活跃项目
- 每个项目下一步是什么
- 某个专题最近做到哪里了

适合：

- 想让项目状态长期可追踪
- 不想把所有信息都埋在聊天记录里

### 场景 4：在飞书里直接看项目和任务看板

系统会把 Vault 里的结构化事实，自动投影成飞书多维表格只读看板。
你可以在手机里直接看：

- 项目总览
- 当前任务
- 阻塞项
- 最近更新

适合：

- 不开 Electron 也想随时看项目状态
- 想和别人分享只读项目看板

### 场景 5：需要本地控制台时，再打开 Electron

Electron 不是这套系统唯一入口。
它更像一个本地工作台，用来：

- 看线程
- 看上下文
- 看本地服务状态
- 做桌面端交互

如果你平时主要用 `Codex` 和 `Feishu`，完全可以不把 Electron 当成第一入口。

## 这套系统现在是正常运行的吗

是。当前这份公开版已经在本地跑通过：

- 一键初始化：`bootstrap`
- 一键验收：`acceptance`
- 去个人化扫描
- 记忆索引刷新
- dashboard 一致性校验
- Feishu bridge runtime 测试

也就是说，这不是一个“概念模板”，而是一份**已经跑通的可部署版本**。  
你要像我现在这样使用，关键不是再写代码，而是按下面流程完成部署和少量授权。

公开版当前已经尽量复刻旗舰版的核心使用体验：

- `CoCo` 继续作为远程入口语义
- `Codex Hub Core` 继续负责记忆、路由、approval、execution、wake 和 writeback
- `GFlow` 推荐、workflow-aware 入口、`Program Harness + Wake Loop`、health wake/catch-up 都已经进入主线
- `Feishu` 侧保留 callback / recovery / execution lease 这套运行时语义
- 平台动作继续通过带风险边界的 `OpenCLI` 执行面承接


### 场景 6：在 Feishu 里远程推进项目，而不是只发命令

你可以在 Feishu 里直接说：

- 继续 `Codex Hub` 这个项目
- 看看 `知识库` 当前做到哪里了
- 帮我汇总一下今天的变更

然后系统会：

- 路由到对应项目
- 读取项目协议和记忆
- 执行任务
- 回传结果卡或摘要

### 场景 7：在 Feishu 里处理需要确认的动作

如果任务涉及更高风险动作，例如：

- 需要更高权限
- 需要系统级操作
- 需要明确批准

那么公开版现在的消息层会把这件事更清楚地呈现出来，而不是只给一段难读的文字。

### 场景 8：复杂任务里让 Codex 给你第二意见

你可以让它：

- 审一下当前方案
- 再给一个反方意见
- 再给一个顾问式建议

这轮公开版已经把这条主链也带进来了，所以这类“不是只执行，而是帮你判断和复审”的工作方式，也已经进入公开版。

## 长任务现在怎么用

公开版现在已经包含 `Program Harness + Wake Loop`。

对人类用户来说，最重要的不是学习一套新命令，而是继续按原来的自然语言方式说清楚三件事：

- 项目范围
- 想推进到什么目标
- 哪些动作需要边界或批准

例如：

- “只在 `TINT` 项目里推进首页改版，先做到可验收。”
- “继续推进 `Codex Hub` 的 Feishu 迁移，涉及外发前先停在批准。”

系统会把这类请求变成一个项目级 program，并在后续 wake 中持续推进：

- 读项目事实源
- 选择当前最重要的子目标
- 执行并验证
- 写回项目板、报告和远程入口
- 判断继续、阻塞、切阶段还是完成

所以公开版现在已经不只是“消息收发 + 对象操作”，而是能承载持续推进的长任务运行时。

## 普通用户从零到可用的完整流程

下面是最推荐的顺序。按这个顺序做，最不容易踩坑。

在开始之前，先记住一条最重要的建议：

- **让 Codex 来执行这套部署流程，通常效果最好**
- **人类用户主要负责授权，而不是手工做所有技术步骤**

### 第 1 步：克隆仓库

```bash
git clone https://github.com/frank28mm/codex-hub.git
cd codex-hub
```

### 第 1.5 步：普通用户可直接双击安装/验收

如果你不想先记命令，公开版已经提供：

- [Install Codex Hub.command](./Install%20Codex%20Hub.command)
- [Validate Codex Hub.command](./Validate%20Codex%20Hub.command)

推荐顺序：

1. 先双击 `Install Codex Hub.command`
2. 再双击 `Validate Codex Hub.command`

如果你本来就是让 `Codex` 帮你部署，也可以直接跳过这两个 `.command` 文件，让它在 `workspace/` 里执行 bootstrap 和 acceptance。

### 第 2 步：确认目录结构

确保这两个目录都在：

- `workspace/`
- `memory/`

其中：

- `workspace/` 是程序本体
- `memory/` 是模板化记忆库

### 第 3 步：安装本地依赖

先确保本机可用：

- `python3`
- `node`
- `codex`

如果 `codex` 还没登录，先做一次：

```bash
codex login
```

如果你后面打算继续用 `Codex` 来帮你部署、接 Feishu、跑验收，那么这一步尤其重要。

### 第 4 步：确认你理解当前依赖边界

- `Codex CLI`：硬依赖
- `Obsidian`：不是硬依赖，但强烈建议安装
- `Feishu`：只有在你要远程协作和只读看板时才需要
- `Electron`：可选入口，不是唯一入口

### 第 5 步：检查站点配置

打开：

- [workspace/control/site.yaml](./workspace/control/site.yaml)

默认推荐保持：

- `workspace_root: auto`
- `memory_root: auto`

这样系统会自动把：

- 当前 `workspace/`
- 旁边的 `memory/`

视为一套可运行环境。

### 第 6 步：执行一键初始化

进入 `workspace/`：

```bash
cd workspace
python3 ops/bootstrap_workspace_hub.py init
```

这个命令会自动：

- 生成本地 `.codex/config.toml`
- 建立 `runtime/`、`logs/`、`reports/ops/`
- 检查并补齐 `memory/` 骨架
- 执行：
  - `refresh-index`
  - `rebuild-all`
  - `verify-consistency`

如果你确定要把后台自动任务也一起装上，再执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-launchagents
```

如果你已经确定要接 Feishu 聊天入口，还可以继续执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge
```

### 第 7 步：执行一键验收

```bash
python3 ops/accept_product.py run
```

通过后，说明这套系统至少已经具备：

- 正确目录结构
- 可用命令
- 无个人路径污染
- 可运行 bootstrap 状态

### 第 8 步：按你的使用目标选择模式

#### 只想先本地使用

这时你已经可以开始用了。  
你可以：

- 在 `workspace/` 下直接开 `Codex`
- 用 `start-codex`
- 用 `Obsidian` 打开 `memory/`

#### 想像我一样用 Feishu 协作

继续完成下面几步。

#### 想增加一个微信私聊入口

在完成本地版之后，你也可以补一个微信私聊版 `CoCo`。

当前范围是：

- 只支持微信私聊
- 不支持群聊
- 走同一条 `Codex Hub` 主链

最推荐方式是：

- 在 `workspace/` 里直接让 `Codex` 帮你接这条入口
- `Codex` 会先启动二维码登录
- 然后等你扫码
- 再继续安装后台常驻并验证状态

完成后，你就可以把微信私聊当成第二个远程入口来用。

### 第 9 步：创建你自己的 Feishu 应用

这里有一个很重要的点：

- `CoCo` 只是我自己的机器人名字
- **你自己创建应用时，可以用任何你喜欢的名字**

也就是说，文档里提到的机器人名字只是示例，不是系统硬编码要求。

你需要的是：

1. 在飞书开放平台创建你自己的应用
2. 给它配置需要的 scope
3. 后面把资源信息写进本地配置

在这一步，最推荐的方式仍然是：

- 让 `Codex` 帮你逐项完成操作说明和本地配置
- 人类自己只负责在飞书页面点确认、授权和审核

### 第 10 步：填写 Feishu 资源模板

打开：

- [workspace/control/feishu_resources.yaml](./workspace/control/feishu_resources.yaml)

至少补这些：

- `owner_open_id`
- 默认 `calendar_id`
- 文档目录或默认访问范围
- 常用表格别名
- 只读投影表格资源

### 第 11 步：完成官方 Feishu CLI 配置与 OAuth

如果你想尽量一键化地完成 Feishu 接入，先执行：

```bash
python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app
```

这一步会尽量把下面几件事串起来：

- 安装官方 `lark-cli`
- 安装官方 `lark-*` skills
- 打开 Feishu 应用创建/配置
- 自动把 `site.yaml` 切到 `feishu_enabled: true`
- 自动同步公开版运行时需要的 `app_id`
- 自动执行公开版统一的 Feishu 登录链
- 最后输出当前 `object_ops_ready / coco_bridge_ready / full_ready` 状态
- 公开版不再把系统钥匙串验证当成默认登录前置步骤

这里要特别注意：

- `object_ops_ready=true` 代表官方 CLI 的对象能力已可用
- `coco_bridge_ready=true` 代表 `CoCo` bridge 也拿到了所需凭据
- 只有 `full_ready=true`，公开版才算 **Feishu 完整可用**

如果 `setup-feishu-cli` 结束后还没有到 `full_ready=true`，先在 `workspace/` 里执行：

```bash
python3 ops/feishu_agent.py auth status
```

如果你明确要排查原生 `lark-cli` 身份，再手动执行：

```bash
lark-cli auth login --domain event,im,docs,drive,base,task,calendar,vc,minutes,contact,wiki,sheets,mail
lark-cli doctor
```

所以正常情况下，不需要每次重新授权。

这里同样建议：

- 由 `Codex` 来驱动这一步
- 人类只在浏览器里完成最终授权

### 第 12 步：补 Feishu bridge 配置并启动协作

公开版已经自带：

- `workspace/ops/feishu_bridge.py`
- `workspace/ops/feishu_bridge.env.example`
- `workspace/bridge/feishu/*`
- `workspace/bridge/feishu_long_connection_service.js`

你要做的是：

1. 先跑一次：
   - `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app`
2. 确认它最后输出：
   - `object_ops_ready=true`
   - `coco_bridge_ready=true`
   - `full_ready=true`
3. 然后启动或安装 bridge

当前最简便的正式方式是：

1. 一个你自己创建的 Feishu 应用
2. 一条官方 `lark-cli` 配置与登录链
3. 公开版仓库自带的 Feishu 长连接 bridge runtime
4. 可选的只读 Bitable 投影

在当前产品里，最推荐的宿主仍然是 Electron，因此通常这样启动：

```bash
cd apps/electron-console
npm install
npm run bridge:install
npm run bridge:status
```

如果你只想验证 bridge runtime 本身，公开版也已经把这层代码直接放进了 `workspace/bridge/`，不再依赖私有仓外部路径。

如果你还想直接打开桌面工作台：

```bash
npm run workspace
```

### 第 13 步：日常怎么使用

部署完成后，最推荐的日常使用方式是：

1. 平时在 `workspace/` 下直接使用 `Codex`
2. 需要项目记忆时，让系统自动读写 `memory/`
3. 需要远程协作时，在 Feishu 里找你自己创建的机器人
4. 需要手机查看项目/任务看板时，看飞书多维表格
5. 需要本地控制台时，打开 Electron

如果你只想先快速体验，最简单的三种起步方式是：

1. 只跑本地版：
   - `codex login`
   - `bootstrap`
   - `acceptance`
   - 然后直接在 `workspace/` 里开 `Codex`
2. 跑本地 + Obsidian：
   - 在上面的基础上，用 `Obsidian` 打开 `memory/`
3. 跑本地 + Feishu：
   - 在上面的基础上，创建你自己的 Feishu 应用并完成一次 OAuth
4. 跑本地 + 微信私聊：
   - 在本地版基础上，执行一次微信扫码登录并安装 `weixin_bridge` 的 LaunchAgent

## Feishu 接入是不是最简便

如果目标是同时保留这些能力：

- 在 Feishu 里聊天
- 让 Codex 操作飞书消息、日历、任务、文档、多维表格、会议
- 自动把 Vault 里的项目/任务投影成飞书只读看板

那么当前这份产品里采用的就是**当前最简便的可工作接入方式**：

1. 一个 Feishu 应用
2. 一次 `OAuth` 登录
3. 仓库内自包含的 Feishu bridge runtime
4. 可选的只读 Bitable 投影

它不是“零配置”，因为：

- Feishu 平台权限审核本身要人工做
- 第一次 OAuth 也要人工确认

但在保留完整能力的前提下，这已经是当前最小、最稳的一条线。

并且对普通用户来说，最省心的实际使用方式是：

- **让 Codex 帮你完成 Feishu 机器人的本地接入、资源配置、bridge 安装和后续对象操作**
- **人类用户只负责必要的页面授权**

## 这份版本不包含什么

- 你的个人项目源码副本
- 你的真实 Obsidian 长期记忆
- 你的真实 Feishu `open_id / calendar_id / app_token / table_id`
- 你个人机器上的运行时缓存和 token

## 进一步说明

更细的代码层与部署说明见：

- [workspace/README.md](./workspace/README.md)
- [memory/README.md](./memory/README.md)
