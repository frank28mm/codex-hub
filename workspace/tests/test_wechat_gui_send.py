import json
import os
from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
SKILL_PATH = REPO_ROOT / "skills" / "wechat-gui-send" / "SKILL.md"
OPS_PATH = REPO_ROOT / "ops" / "wechat_gui_send.py"


def test_repo_includes_wechat_gui_send_skill() -> None:
    text = SKILL_PATH.read_text(encoding="utf-8")
    assert "name: wechat-gui-send" in text
    assert "Computer Use" in text
    assert "prepare -> confirm send" in text
    assert "Use the WeChat search field to paste the recipient name for fast targeting." in text
    assert "Paste the message text from the system clipboard" in text
    assert "Press `Return` to send the message." in text


def test_prepare_queue_persists_items(sample_env) -> None:
    from ops import wechat_gui_send

    payload = wechat_gui_send.prepare_queue(
        [
            {"recipient_name": "张三", "message_text": "你好，今晚联系你。"},
            {"recipient_name": "李四", "message_text": "资料我已经整理好了。"},
        ]
    )
    queue_path = (
        sample_env["runtime_root"]
        / "wechat-gui-send"
        / "queues"
        / f"{payload['queue_id']}.json"
    )
    assert queue_path.exists()
    assert payload["status"] == "prepared"
    assert [item["recipient_name"] for item in payload["items"]] == ["张三", "李四"]


def test_confirm_queue_requires_explicit_confirmation(sample_env) -> None:
    from ops import wechat_gui_send

    payload = wechat_gui_send.prepare_queue(
        [{"recipient_name": "张三", "message_text": "你好"}]
    )
    blocked = wechat_gui_send.confirm_queue(payload["queue_id"], confirmed=False)
    confirmed = wechat_gui_send.confirm_queue(payload["queue_id"], confirmed=True)
    assert blocked["error"] == "confirmation_required"
    assert confirmed["status"] == "confirmed"


def test_review_queue_returns_preview(sample_env) -> None:
    from ops import wechat_gui_send

    payload = wechat_gui_send.prepare_queue(
        [{"recipient_name": "张三", "message_text": "今晚 8 点给你电话。"}]
    )
    review = wechat_gui_send.review_queue(payload["queue_id"])
    assert review["status"] == "prepared"
    assert review["total_items"] == 1
    assert review["items"][0]["message_preview"] == "今晚 8 点给你电话。"


def test_cli_prepare_and_review_round_trip(sample_env) -> None:
    env = os.environ.copy()
    env["WORKSPACE_HUB_ROOT"] = str(sample_env["workspace_root"])
    env["WORKSPACE_HUB_RUNTIME_ROOT"] = str(sample_env["runtime_root"])
    items_json = json.dumps(
        [{"recipient_name": "张三", "message_text": "请今晚看一下消息。"}],
        ensure_ascii=False,
    )
    prepare = subprocess.run(
        [
            "python3",
            str(OPS_PATH),
            "prepare",
            "--items-json",
            items_json,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert prepare.returncode == 0, prepare.stderr
    prepared_payload = json.loads(prepare.stdout)

    review = subprocess.run(
        [
            "python3",
            str(OPS_PATH),
            "review",
            "--queue-id",
            prepared_payload["queue_id"],
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert review.returncode == 0, review.stderr
    reviewed_payload = json.loads(review.stdout)
    assert reviewed_payload["total_items"] == 1
    assert reviewed_payload["items"][0]["recipient_name"] == "张三"
