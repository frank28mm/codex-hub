# Codex Hub

`Codex Hub` 是一套以 `Codex + Obsidian + Feishu` 为核心的本地工作系统。

它解决的不是“单次聊天”，而是：

- 在本地长期维护项目和任务真相
- 通过 `Codex` 执行项目级工作
- 通过 `Feishu` 做远程协作、日程/任务/文档/多维表格操作
- 把结果自动写回记忆系统，并同步成手机可查看的只读看板

这份仓库是 `Codex Hub` 的**可复制、可公开、可本地部署**版本，不包含任何个人真实记忆、真实飞书资源或私有项目源码。

## 当前正式支持

- **正式支持：macOS**
- 当前这版后台自动化依赖 `launchd / LaunchAgents`
- Windows 目前**不作为正式支持平台**

## 仓库结构

- `workspace/`
  - 产品代码、启动器、Feishu 能力、Electron 前端、自动化脚本、测试和系统文档
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
3. 用 `Feishu` 做远程协作入口
4. 用飞书多维表格看手机端只读项目看板

## 这套系统现在是正常运行的吗

是。当前这份公开版已经在本地跑通过：

- 一键初始化：`bootstrap`
- 一键验收：`acceptance`
- 去个人化扫描
- 记忆索引刷新
- dashboard 一致性校验

也就是说，这不是一个“概念模板”，而是一份**已经跑通的可部署版本**。  
你要像我现在这样使用，关键不是再写代码，而是按下面流程完成部署和少量授权。

## 普通用户从零到可用的完整流程

下面是最推荐的顺序。按这个顺序做，最不容易踩坑。

### 第 1 步：克隆仓库

```bash
git clone https://github.com/frank28mm/codex-hub.git
cd codex-hub
```

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

### 第 4 步：检查站点配置

打开：

- [workspace/control/site.yaml](./workspace/control/site.yaml)

默认推荐保持：

- `workspace_root: auto`
- `memory_root: auto`

这样系统会自动把：

- 当前 `workspace/`
- 旁边的 `memory/`

视为一套可运行环境。

### 第 5 步：执行一键初始化

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

### 第 6 步：执行一键验收

```bash
python3 ops/accept_product.py run
```

通过后，说明这套系统至少已经具备：

- 正确目录结构
- 可用命令
- 无个人路径污染
- 可运行 bootstrap 状态

### 第 7 步：按你的使用目标选择模式

#### 只想先本地使用

这时你已经可以开始用了。  
你可以：

- 在 `workspace/` 下直接开 `Codex`
- 用 `start-codex`
- 用 `Obsidian` 打开 `memory/`

#### 想像我一样用 Feishu 协作

继续完成下面几步。

### 第 8 步：创建你自己的 Feishu 应用

这里有一个很重要的点：

- `CoCo` 只是我自己的机器人名字
- **你自己创建应用时，可以用任何你喜欢的名字**

也就是说，文档里提到的机器人名字只是示例，不是系统硬编码要求。

你需要的是：

1. 在飞书开放平台创建你自己的应用
2. 给它配置需要的 scope
3. 后面把资源信息写进本地配置

### 第 9 步：填写 Feishu 资源模板

打开：

- [workspace/control/feishu_resources.yaml](./workspace/control/feishu_resources.yaml)

至少补这些：

- `owner_open_id`
- 默认 `calendar_id`
- 文档目录或默认访问范围
- 常用表格别名
- 只读投影表格资源

### 第 10 步：完成一次 Feishu OAuth

在 `workspace/` 里执行：

```bash
python3 ops/feishu_agent.py auth login
```

这一步的目标是：

- 完成一次用户身份授权
- 后续由系统自动续期

所以正常情况下，不需要每次重新授权。

### 第 11 步：启动 Feishu 协作

当前最简便的正式方式是：

1. 一个 Feishu 应用
2. 一次 OAuth 登录
3. 一个 Electron 宿主内的长连接桥接服务
4. 可选的只读 Bitable 投影

如果你要启用这条线，进入：

```bash
cd apps/electron-console
npm install
npm run bridge:install
npm run bridge:status
```

如果你还想直接打开桌面工作台：

```bash
npm run workspace
```

### 第 12 步：日常怎么使用

部署完成后，最推荐的日常使用方式是：

1. 平时在 `workspace/` 下直接使用 `Codex`
2. 需要项目记忆时，让系统自动读写 `memory/`
3. 需要远程协作时，在 Feishu 里找你自己创建的机器人
4. 需要手机查看项目/任务看板时，看飞书多维表格
5. 需要本地控制台时，打开 Electron

## Feishu 接入是不是最简便

如果目标是同时保留这些能力：

- 在 Feishu 里聊天
- 让 Codex 操作飞书消息、日历、任务、文档、多维表格、会议
- 自动把 Vault 里的项目/任务投影成飞书只读看板

那么当前这份产品里采用的就是**当前最简便的可工作接入方式**：

1. 一个 Feishu 应用
2. 一次 `OAuth` 登录
3. 一个 Electron 宿主内的长连接桥接服务
4. 可选的只读 Bitable 投影

它不是“零配置”，因为：

- Feishu 平台权限审核本身要人工做
- 第一次 OAuth 也要人工确认

但在保留完整能力的前提下，这已经是当前最小、最稳的一条线。

## 这份版本不包含什么

- 你的个人项目源码副本
- 你的真实 Obsidian 长期记忆
- 你的真实 Feishu `open_id / calendar_id / app_token / table_id`
- 你个人机器上的运行时缓存和 token

## 进一步说明

更细的代码层与部署说明见：

- [workspace/README.md](./workspace/README.md)
- [memory/README.md](./memory/README.md)
