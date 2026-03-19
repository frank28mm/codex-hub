# Codex Hub

这是 `Codex Hub` 的**可复制产品化备份版本**。

目录分为两层：

- `workspace/`
  - 产品代码、控制层、启动器、Feishu 能力、Electron 前端、测试与系统说明
- `memory/`
  - 模板化记忆骨架，不包含个人真实长期记忆

## 适用场景

这份仓库用于：

1. 本地初始化一个新的 `Codex Hub` 工作系统
2. 基于模板化 memory 运行 `start-codex`
3. 按需开启 Feishu 协作与只读 Bitable 看板
4. 作为后续公开版本和产品化部署的基础

## 快速开始

进入 `workspace/` 后运行：

```bash
python3 ops/bootstrap_workspace_hub.py init
python3 ops/accept_product.py run
```

更完整的部署、授权和运行说明见：

- [workspace/README.md](./workspace/README.md)

## 当前边界

- 这是抽离版产品层，不是任何个人现网工作区的直接镜像
- 不包含个人 Feishu 资源、个人 token、个人长期记忆和私有项目副本
- 首次使用仍需要少量人工授权，例如 `codex login` 与 Feishu OAuth
