from __future__ import annotations

import os


DEFAULT_ASSISTANT_NAME = "CoCo"


def assistant_name() -> str:
    return str(os.environ.get("WORKSPACE_HUB_ASSISTANT_NAME", "")).strip() or DEFAULT_ASSISTANT_NAME


def assistant_private_thread_label() -> str:
    return f"{assistant_name()} 私聊"


def assistant_service_label() -> str:
    return f"{assistant_name()} 服务"


def assistant_customization_hint() -> str:
    return (
        f"默认机器人昵称是 {assistant_name()}。"
        "如需自定义，可设置环境变量 WORKSPACE_HUB_ASSISTANT_NAME。"
    )


def feishu_private_target() -> str:
    return str(os.environ.get("WORKSPACE_HUB_FEISHU_PRIVATE_TARGET", "")).strip()
