---
name: feishu-ops
description: Use when Codex needs to operate Feishu/Lark objects from the Codex Hub workspace, including messages, users, documents, bitables, calendars, tasks, and video meetings via the local broker.
---

# Feishu Ops

Use this skill when the user wants Codex to **operate Feishu objects**, not just discuss them.

## Fixed Paths

1. Broker: `ops/local_broker.py`
2. Adapter: `ops/feishu_agent.py`
3. Resource registry: `control/feishu_resources.yaml`

## Core Rule

If the user asks to create, update, delete, send, schedule, or query a Feishu object, prefer:

`python3 ops/local_broker.py feishu-op ...`

Do not stop at explanation when an operation is possible.

## Supported Domains

1. `msg`
   - `send`
   - `reply`
   - `history`
   - `search`
   - `chats`
2. `user`
   - `get`
   - `search`
3. `doc`
   - `create`
   - `get`
   - `list`
4. `table`
   - `create-app`
   - `create`
   - `create-field`
   - `records`
   - `add`
   - `update`
   - `delete`
   - `tables`
   - `fields`
5. `cal`
   - `list`
   - `add`
   - `delete`
6. `task`
   - `list`
   - `add`
   - `done`
   - `delete`
7. `meeting`
   - `create`
   - `get`
   - `list`
   - `delete`
   - `cancel`

## Resource Resolution

Before asking the user for raw IDs, check the resource registry:

- default calendar
- common chats / users
- bitable app/table aliases
- document folder aliases

If the registry does not contain the target:

1. parse IDs from Feishu URLs when possible
2. resolve users by email or name
3. resolve chats by group name

## Usage Pattern

Use JSON payloads through the broker, for example:

```bash
python3 ops/local_broker.py feishu-op \
  --domain task \
  --action add \
  --payload-json '{"title":"准备季度述职","due":"2026-03-25 18:00","note":"准备PPT和数据"}'
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain cal \
  --action add \
  --payload-json '{"title":"产品评审会","start":"2026-03-18 15:00","end":"2026-03-18 16:00","location":"3楼大会议室"}'
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain meeting \
  --action create \
  --payload-json '{"title":"产品项目周会","start":"2026-03-18 19:00","end":"2026-03-18 19:30","attendees":["user@example.com"]}'
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain table \
  --action create-app \
  --payload-json '{"name":"2026 阅读书单","table_name":"书单","fields":[{"field_name":"书名","type":1},{"field_name":"状态","type":3}]}' 
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain table \
  --action create \
  --payload-json '{"app":"https://feishu.cn/base/APP_TOKEN","name":"需求池","fields":[{"field_name":"标题","type":1},{"field_name":"状态","type":3},{"field_name":"截止日期","type":5}]}' 
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain table \
  --action create-field \
  --payload-json '{"table":"书单","field_name":"作者","type":1}'
```

```bash
python3 ops/local_broker.py feishu-op \
  --domain table \
  --action add \
  --payload-json '{"table":"书单","data":{"书名":"穷查理宝典","状态":"在读"}}'
```

## Guidance

1. Query before mutating when the target is ambiguous.
2. For destructive operations (`delete`, `cancel`), keep the current workspace approval policy in mind.
3. Return the created object IDs and links when available.
4. If a requested target cannot be resolved, tell the user what alias, URL, or identifier is still missing instead of guessing.
5. For bitable automation, prefer this sequence:
   - `table create-app` for a new base
   - `table create` to add more tables
   - `table create-field` to extend an existing table
   - `table add/update/delete` for record CRUD
6. Feishu bitable can already support:
   - creating a new base app
   - creating tables with initial field definitions
   - adding fields to existing tables
   - listing tables/fields/records
   - record CRUD on existing tables
