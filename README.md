# Codex Hub

`Codex Hub` 是一个以 `Codex + Obsidian + Feishu` 为核心的本地工作系统。

它解决的不是“单次问答”，而是：

- 在本地长期维护项目和任务真相
- 通过 `Codex` 执行项目级工作
- 通过 `Feishu` 远程协作、管理日程/任务/文档/多维表格
- 把结果自动写回记忆系统，并同步成手机可查看的只读看板

这份仓库是 `Codex Hub` 的**可复制、可公开、可本地部署**版本，不包含任何个人真实记忆、真实飞书资源或私有项目源码。

## 仓库结构

- `workspace/`
  - 产品代码、启动器、Feishu 能力、Electron 前端、自动化脚本、测试和系统文档
- `memory/`
  - 模板化的长期记忆库骨架，供新部署者本地初始化使用

## 它适合谁

适合想要在本地搭建以下工作方式的人：

1. 用 `Codex` 驱动日常项目执行
2. 用 `Obsidian` 维护项目记忆和事实源
3. 用 `Feishu` 做远程协作入口和只读移动端看板
4. 需要一套能逐步扩展到：
   - Electron 桌面控制台
   - Feishu 聊天协作
   - 飞书对象操作
   - 只读 Bitable 看板
   的工作系统

## 依赖与官方链接

部署前你至少需要了解或安装以下产品：

- `Codex`
  - 官方文档：[OpenAI Codex](https://developers.openai.com/codex/)
- `Feishu`
  - 开放平台：[Feishu Open Platform](https://open.feishu.cn/)
  - 产品官网：[飞书](https://www.feishu.cn/)
- `Obsidian`
  - 官网：[Obsidian](https://obsidian.md/)

本地运行还需要：

- `Python 3`
- `Node.js`
- `Codex CLI`
- macOS
  - 当前这一版的自动后台任务依赖 `launchd`

## 最快上手

1. 克隆本仓库
2. 进入 [workspace/README.md](./workspace/README.md)，按“本地单机版”完成初始化
3. 运行：

```bash
python3 ops/bootstrap_workspace_hub.py init
python3 ops/accept_product.py run
```

如果你只想先在本地使用 `Codex + memory`，做到这一步就够了。  
如果你还想接入 `Feishu`，请继续看 `workspace/README.md` 里的 Feishu 部分。

## Feishu 接入是不是最简便

如果目标是同时保留这些能力：

- `Feishu` 里直接和 CoCo 对话
- 让 Codex 去操作飞书消息、日历、任务、文档、多维表格、会议
- 自动把 Vault 里的项目/任务投影成飞书只读看板

那么这份产品里现在采用的就是**当前最简便的可工作接入方式**：

1. 一个 Feishu 应用
2. 一次 `OAuth` 登录
3. 一个 Electron 宿主内的长连接桥接服务
4. 可选开启只读 Bitable 投影

它不是“零配置”，因为 Feishu 平台权限审核和 `OAuth` 本身就需要人工完成；但在保留完整能力的前提下，这已经是当前最简的正式方案。

## 这份版本不包含什么

- 你的个人项目源码副本
- 你的真实 Obsidian 长期记忆
- 你的真实 Feishu `open_id / calendar_id / app_token / table_id`
- 你个人机器上的运行时缓存和 token

## 下一步看哪里

- 本地部署与使用说明：
  [workspace/README.md](./workspace/README.md)
- 模板记忆库说明：
  [memory/README.md](./memory/README.md)
