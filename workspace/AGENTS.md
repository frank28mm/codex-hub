# Codex Hub Product AGENTS

1. 当前目录是 `Codex Hub` 的产品化备份工作层，不是现网工作区。
2. 这里的目标是抽离可复制、可部署、可公开的系统骨架。
3. 默认 workspace root 是当前仓库根目录。
4. 默认 memory root 是同级目录 `../memory/`。
5. 不要把个人真实飞书资源、token、open_id、真实 app_token/table_id 写入正式模板。
6. 不要把个人真实 Vault 内容、私有项目副本或项目源码带入产品模板。
7. `ops/start-codex` 仍是统一强入口，但产品化版本最终应优先依赖可配置 site root，而不是个人固定路径。
8. 所有需要公开分发的说明，应优先写到 `README.md` 或 `reports/system/`。
9. 对外分发前，优先做：
   - 去个人化
   - bootstrap 初始化
   - acceptance 验收
10. 当前目录可以继续作为“产品化抽离工作区”迭代，但在正式公开前不应承载个人长期记忆或私有运行态。

