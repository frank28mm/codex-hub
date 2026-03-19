# Codex Hub Workspace

这是 `Codex Hub` 的工作层。  
它负责运行这套系统的核心能力：

- 统一启动 `Codex`
- 读取和写回 `Obsidian` 记忆库
- 通过 `Feishu` 做远程协作
- 通过 `Electron` 提供本地控制台
- 通过自动化脚本维护 dashboard、watcher、只读 Bitable 看板

如果你把整个仓库当成一个产品来看：

- `workspace/` 是“代码和运行层”
- `memory/` 是“模板化的长期记忆层”

## 这是什么工具

`Codex Hub` 不是普通脚手架。它是一套本地优先的工作系统，适合：

1. 用 `Codex` 处理项目任务
2. 用 `Obsidian` 保存长期项目记忆和任务真相
3. 用 `Feishu` 做远程协作入口
4. 在手机端通过飞书只读看板查看项目和任务

## 典型使用场景

### 场景 1：个人项目工作台

你在 `workspace/` 下直接使用 `Codex`，让它持续处理某个项目。
系统会自动结合 `memory/` 中的项目事实和上下文，而不是每次都从零开始。

### 场景 2：手机上的远程协作入口

你在 Feishu 里对自己创建的机器人发一句话：

- 创建任务
- 安排日程
- 新建文档
- 新建或更新多维表格
- 继续某个项目工作

这时 Feishu 只是入口，底层仍然是同一套 `Codex Hub`。

### 场景 3：长期记忆 + 可视化看板

项目事实长期保存在 sibling `memory/` 中；
同时又会自动投影到飞书多维表格，方便你在手机端查看项目总览和当前任务。

### 场景 4：桌面控制台

如果你需要本地线程视图、上下文抽屉或服务控制面，可以打开 Electron。
但这不是唯一入口，只是桌面工作台。

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

### 官方链接

- `Codex`：[OpenAI Codex](https://developers.openai.com/codex/)
- `Feishu` 开放平台：[Feishu Open Platform](https://open.feishu.cn/)
- `Feishu` 产品官网：[飞书](https://www.feishu.cn/)
- `Obsidian`：[Obsidian](https://obsidian.md/)

## 目录说明

- `ops/`
  - 启动器、broker、watcher、dashboard sync、Feishu 工具层、只读投影等脚本
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
> `Codex` 只要从当前 `workspace/` 启动，就会自动读取：
> - [AGENTS.md](/Users/frank/Codex Hub/workspace/AGENTS.md)
> - [MEMORY_SYSTEM.md](/Users/frank/Codex Hub/workspace/MEMORY_SYSTEM.md)
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
- 旁边同级的 `memory/`

如果你不想改目录结构，通常不需要改这两个值。

### 3. 执行一键初始化

```bash
python3 ops/bootstrap_workspace_hub.py init
```

这个命令会：

- 生成本地 `.codex/config.toml`
- 建立 `runtime/`、`logs/`、`reports/ops/`
- 确保 sibling `memory/` 骨架存在
- 执行：
  - `refresh-index`
  - `rebuild-all`
  - `verify-consistency`
- 输出：
  - `runtime/bootstrap-status.json`

如果你已经确认要把后台自动任务也一起装上，再运行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-launchagents
```

如果你已经准备好启用 Feishu 聊天入口，还可以继续执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge
```

### 4. 执行一键验收

```bash
python3 ops/accept_product.py run
```

验收会检查：

- 路径是否完整
- `python3 / node / codex` 是否可用
- 是否还残留个人现网路径
- bootstrap 是否完成

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
系统可以直接读写 `memory/` 文件；你只是在需要人类查看、深链跳转和长期浏览时再打开 `Obsidian`。

#### Feishu

如果你暂时不需要 Feishu，可以跳过这一段，先直接使用本地版。

如果你需要 Feishu 协作，请继续：

1. 在 [control/site.yaml](./control/site.yaml) 里把：
   - `feishu_enabled: true`
2. 打开 [control/feishu_resources.yaml](./control/feishu_resources.yaml)
3. 填入你的：
   - `owner_open_id`
   - 默认 `calendar_id`
   - 文档目录
   - 表格别名
   - 只读投影资源
4. 用一次 OAuth 登录：

```bash
python3 ops/feishu_agent.py auth login
```

5. 确保你的 Feishu 应用 scope 已经通过审核并发布
6. 复制：
   - `ops/feishu_bridge.env.example`
   - 到 `ops/feishu_bridge.env.local`
7. 然后执行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge
```

对普通用户来说，最省事的方式不是手工自己逐项配，而是：

- 让 `Codex` 来帮你完成飞书机器人接入、资源模板填写、OAuth 流程引导、bridge 安装和验证
- 你自己只在飞书页面完成必要确认

## Feishu 最简接入方式

如果你想要的能力只有：

- 在 Feishu 里聊天
- 让 Codex 操作飞书对象
- 在飞书里看只读项目/任务看板

那么当前仓库采用的就是**最简便的可工作方案**：

1. **一个 Feishu 应用**
2. **一次 OAuth 登录**
3. **一个 Electron 宿主内的长连接桥接服务**
4. **一个可选的只读 Bitable 投影**

也就是说，没有额外的 sidecar 编排层，没有第二套数据库，也没有独立的 web 管理后台。

它不是“绝对零配置”的最简单方案，因为 Feishu 平台权限审核本身就需要人工处理；但在保留完整能力的前提下，这已经是当前最小、最稳的一条线。

### 如何启动 Feishu 协作

当前推荐方式是通过 Electron 宿主运行 Feishu 长连接桥接。

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

如果你只是想先本地看 Electron 工作台，也可以：

```bash
npm run workspace
```

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

## 日常使用方式

最推荐的工作方式是：

1. 平时直接在 `workspace/` 下使用 `Codex`
2. 需要记忆时让系统自动读写 `memory/`
3. 需要远程协作时用 Feishu 找你自己创建的机器人
4. 需要看项目和任务可视化时看 Feishu Bitable
5. 需要本地控制台时打开 Electron

如果你是第一次上手，建议按这个顺序体验：

1. 先只跑本地版，确认 `bootstrap + acceptance` 都通过
2. 再打开 `Obsidian` 看 `memory/` 结构
3. 最后再接入 Feishu 和只读 Bitable，看远程协作体验

## 常用命令

### 初始化与验收

```bash
python3 ops/bootstrap_workspace_hub.py init
python3 ops/bootstrap_workspace_hub.py status
python3 ops/accept_product.py run
```

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

## 当前边界

- 这是可部署版本，不是你的个人现网镜像
- 默认不带任何真实飞书资源和 token
- 默认不带真实长期记忆
- Feishu 权限审核和第一次 OAuth 无法完全自动化，只能做到“少量人工授权后长期自动续期”
- 当前正式支持平台是 macOS；Windows 尚未完成后台任务、常驻 bridge、通知与保活适配
