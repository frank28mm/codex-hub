# Codex Hub Public Product AGENTS

这个仓库是 `Codex Hub` 的公开、可复制、可部署版本。

如果你是 coding agent，在这个仓库里工作时，优先遵循下面的部署口径。

## 1. 先理解这不是个人现网镜像

- 这里是产品化模板，不是某个人当前正在使用的私有实例。
- 不要假设任何真实：
  - `open_id`
  - `calendar_id`
  - `app_token`
  - `table_id`
  - 本机路径
  - LaunchAgent 名称
- 默认站点根和 memory 根都应由模板配置自动推导。

## 2. 最佳部署方式：用 Codex 自己完成部署

这套系统最推荐的部署方式不是“人类手工逐项折腾”，而是：

1. 人类把仓库克隆到本机
2. 人类进入 `workspace/`
3. 人类让 `Codex` 直接接手部署
4. `Codex` 负责：
   - 检查目录结构
   - 检查依赖
   - 执行 bootstrap
   - 执行 acceptance
   - 引导 Feishu 资源模板填写
   - 引导或代执行 Feishu bridge 安装
   - 在需要时操作 Feishu 对象能力
5. 人类只在必要时做少量授权：
   - `codex login`
   - Feishu 开放平台权限审核
   - Feishu OAuth 登录确认

也就是说：

- **使用 Codex 来自行部署，效果最佳**
- **人类用户主要负责授权，不需要手工完成大部分技术步骤**

## 3. 你应该如何帮助部署者

如果用户要在本机部署这套系统，优先按这个顺序工作：

1. 检查 `workspace/` 和 `memory/` 是否都存在
2. 读取：
   - `README.md`
   - `workspace/README.md`
   - `workspace/control/site.yaml`
3. 检查：
   - `python3`
   - `node`
   - `codex`
4. 如未登录，提示用户先做：
   - `codex login`
5. 执行：
   - `python3 ops/bootstrap_workspace_hub.py init`
   - `python3 ops/accept_product.py run`
6. 如果用户要接 Feishu：
   - 指导其创建**自己命名**的 Feishu 应用
   - 指导其填写 `workspace/control/feishu_resources.yaml`
   - 指导或代执行 `python3 ops/feishu_agent.py auth login`
   - 指导或代执行 Electron bridge 的安装与状态检查

## 4. 关于 Feishu 的准确描述

- `CoCo` 只是原作者自己的机器人名称示例。
- 部署者可以给自己的 Feishu 机器人起任何名字。
- 你不应把 `CoCo` 写成系统固定产品名。

## 5. 边界

- 当前正式支持平台是 `macOS`
- 当前公开版仍然依赖：
  - `launchd`
  - `LaunchAgents`
  - Electron 宿主 bridge
- `Windows` 目前不作为正式支持平台

## 6. 安全与隐私

- 不要把真实 token、真实用户标识、真实飞书资源 id 提交进仓库
- 不要把个人项目源码、个人长期记忆、个人日志混入模板仓库
- 如果用户要求配置 Feishu，优先写入本地模板文件，不要把其真实资源直接提交到公开仓库
