# Codex Hub Memory Template

这里不是代码仓，而是 `Codex Hub` 的模板化长期记忆层。

它的作用是：

1. 给新部署者提供一套最小可运行的记忆系统骨架
2. 作为 `workspace/` 的 sibling memory 根目录
3. 被 `Codex`、`Obsidian`、watcher、dashboard sync 共同读写

## 这份 memory 不是什么

它不是：

- 某个人真实在用的 Vault
- 任何个人历史项目、私有总结、私有日志的公开副本
- 第二套任务真相源

它只是一个**模板化、可启动、可扩展**的记忆库骨架。

## 普通用户怎么理解这层

如果你把 `workspace/` 看成程序本体，那么 `memory/` 就是：

- 项目注册表
- 活跃项目清单
- 当前动作派生板
- 工作中板面
- 语义页
- dashboard 展示页

系统会自动对它做这些事：

- 写项目板和专题板
- 刷新 `NEXT_ACTIONS`
- 刷新 dashboard
- 记录项目摘要
- 维护只读投影所需的结构化事实

## 推荐配合什么软件

虽然系统不强制要求 GUI，但这层最适合配合 `Obsidian` 查看：

- 官网：[Obsidian](https://obsidian.md/)

你可以：

1. 不开 Obsidian，只让系统直接读写这些 Markdown 文件
2. 需要人工查看时，再用 Obsidian 打开这个 `memory/` 目录

## 当前目录结构

- `PROJECT_REGISTRY.md`
  - 全局项目注册入口
- `ACTIVE_PROJECTS.md`
  - 当前活跃项目入口
- `NEXT_ACTIONS.md`
  - 全局动作派生板
- `01_working/`
  - 项目板、专题板、进行中状态
- `02_episodic/daily/`
  - 每日写回日志
- `03_semantic/projects/`
  - 项目摘要页
- `03_semantic/systems/`
  - 系统说明页
- `07_dashboards/`
  - 自动展示层

## 和 Feishu、Electron 的关系

这层仍然是真相源。  
`Feishu` 和 `Electron` 只是入口与展示层：

- Feishu 可以聊天协作、对象操作、看只读 Bitable 看板
- Electron 可以作为本地控制台

但任务状态和项目事实最终仍然回到这层。

## 初始化与使用

不需要单独初始化这层。  
正常顺序是：

1. 先在 `workspace/` 里运行：

```bash
python3 ops/bootstrap_workspace_hub.py init
```

2. 系统会自动检查并补齐这份 `memory/` 的最小骨架
3. 再运行：

```bash
python3 ops/accept_product.py run
```

4. 之后你就可以：
   - 用 `Codex` 工作
   - 用 `Obsidian` 查看这份记忆库
   - 用 `Feishu` 做远程入口

## 如果你准备公开或复制这套系统

建议保留这份 `memory/` 作为模板，不要把个人真实长期记忆直接放进来。  
真正个人化的项目、日志和历史，应在部署后的本地实例里逐步生成，而不是在模板仓库里直接分发。
