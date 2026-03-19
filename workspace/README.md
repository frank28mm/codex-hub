# Codex Hub

当前目录约定：

- `workspace/`
  - 产品代码、控制层、桌面前端、自动化脚本、测试与说明
- `memory/`
  - 模板化的长期记忆骨架，而不是个人真实 Vault


## 目录结构

- `ops/`
  - 启动器、broker、watcher、dashboard sync、Feishu 工具层等自动化脚本
- `control/`
  - 控制真源、模型默认值、执行档位、目标分类和飞书资源模板
- `apps/`
  - Electron 桌面前端
- `.agents/`
  - repo skills
- `.codex/`
  - Codex 本地运行默认
- `tests/`
  - 回归和 contract tests
- `reports/system/`
  - 产品化与部署说明会逐步沉淀到这里

## 快速开始

### 1. 初始化

在 `workspace/` 根目录运行：

```bash
python3 ops/bootstrap_workspace_hub.py init
```

如果你已经确认要在本机安装 watcher / dashboard sync / health / Feishu projection 的 launchd 任务，再运行：

```bash
python3 ops/bootstrap_workspace_hub.py init --install-launchagents
```

默认行为：

- 生成 `.codex/config.toml`
- 建立 `runtime/`、`logs/`、`reports/ops/` 等运行目录
- 保证 sibling `memory/` 的最小骨架存在
- 执行：
  - `refresh-index`
  - `rebuild-all`
  - `verify-consistency`
- 输出 `runtime/bootstrap-status.json`

### 2. 查看 bootstrap 状态

```bash
python3 ops/bootstrap_workspace_hub.py status
```

### 3. 运行产品级验收

```bash
python3 ops/accept_product.py run
```

验收会生成：

- `reports/system/product-acceptance-latest.md`

### 4. 少量人工授权

本产品需要少量人工步骤：

- `Codex`：
  - 如果当前机器还没登录，执行一次 `codex login`
- `Feishu`：
  - 先在 `control/feishu_resources.yaml` 填入 app、日历、表格和别名
  - 如果启用 Feishu，对象操作首次需要：
    - `python3 ops/feishu_agent.py auth login`
  - 还需要确保开放平台应用 scope 已审核通过

## 部署模型

产品支持 3 种使用层级：

1. **本地单机版**
   - 不启用 Feishu
   - 只使用 `start-codex + memory + watcher + dashboard`
2. **Feishu 协作版**
   - 增加 CoCo bridge 与 Feishu 对象操作
   - 需要人工完成一次 OAuth 和应用权限配置
3. **Bitable 看板版**
   - 再加飞书只读投影
   - 适合手机查看项目和任务看板

## 当前建议使用方式

当前更适合这样用：

1. 先运行 bootstrap
2. 先跑 acceptance
3. 再补 Feishu 资源和 OAuth
4. 最后才做首次公开仓库发布

也就是说，这份目录已经可以作为“本地可初始化的抽离版产品层”继续推进，但公开发布前仍建议先完成一轮本机验收。

## 后续路线

当前产品化路线固定为：

1. 完成本地 bootstrap 与 acceptance 验证
2. 明确 Feishu / Codex 的最小人工授权步骤
3. 做首次公开仓库发布
4. 再继续补部署手册和 post-v1.0.6 的产品化边界说明

## 关键文件

- `control/site.yaml`
  - 产品站点级配置
- `control/feishu_resources.yaml`
  - Feishu 资源模板
- `ops/bootstrap_workspace_hub.py`
  - 一键初始化入口
- `ops/accept_product.py`
  - 产品级验收入口
- `reports/system/product-acceptance-latest.md`
  - 最近一次验收报告
