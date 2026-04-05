from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path


def test_install_python_dependencies_uses_requirements_file(monkeypatch, tmp_path: Path) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("PyYAML\n", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "REQUIREMENTS_PATH", requirements)
    monkeypatch.setattr(bootstrap, "missing_python_packages", lambda: ["PyYAML"])
    monkeypatch.setattr(bootstrap, "python_module_status", lambda: {"yaml": True})
    observed: dict[str, object] = {}

    def fake_run_command(cmd, cwd):
        observed["cmd"] = list(cmd)
        observed["cwd"] = cwd
        return {"returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)

    payload = bootstrap.install_python_dependencies()

    assert payload["installed"] is True
    assert observed["cmd"] == [bootstrap.sys.executable, "-m", "pip", "install", "-r", str(requirements)]
    assert observed["cwd"] == bootstrap.WORKSPACE_ROOT


def test_bootstrap_python_dependency_inventory_covers_acceptance_modules() -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)

    modules = {module for module, _package in bootstrap.PYTHON_DEPENDENCIES}

    assert "cryptography" in modules
    assert "openai" in modules


def test_maybe_install_launchagents_uses_poll_interval_for_watcher(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[list[str]] = []

    def fake_run_command(cmd, cwd, *, timeout_seconds):
        observed.append(list(cmd))
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command_with_timeout", fake_run_command)
    site = bootstrap.default_site_config()

    payload = bootstrap.maybe_install_launchagents(site, install=True)

    assert payload["installed"] is True
    assert observed[0][:4] == ["python3", "ops/codex_session_watcher.py", "install-launchagent", "--poll-interval"]
    assert observed[0][4] == "300"
    assert observed[-1][:3] == ["python3", "ops/knowledge_intake.py", "install-launchagent"]


def test_default_site_config_uses_generated_memory_root(monkeypatch, tmp_path: Path) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    monkeypatch.setattr(bootstrap, "WORKSPACE_ROOT", workspace_root)
    monkeypatch.setattr(bootstrap, "DEFAULT_MEMORY_ROOT", (workspace_root.parent / "memory.local").resolve())

    site = bootstrap.default_site_config()

    assert site.memory_root == (workspace_root.parent / "memory.local").resolve()


def test_seed_memory_template_copies_template_files_without_overwriting(monkeypatch, tmp_path: Path) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    template_root = tmp_path / "memory"
    runtime_root = tmp_path / "memory.local"
    (template_root / "01_working").mkdir(parents=True)
    (template_root / "PROJECT_REGISTRY.md").write_text("template registry\n", encoding="utf-8")
    (template_root / "01_working" / "NOW.md").write_text("template now\n", encoding="utf-8")
    (runtime_root / "PROJECT_REGISTRY.md").parent.mkdir(parents=True, exist_ok=True)
    (runtime_root / "PROJECT_REGISTRY.md").write_text("runtime registry\n", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "MEMORY_TEMPLATE_ROOT", template_root)

    payload = bootstrap.seed_memory_template(runtime_root)

    assert payload["seeded"] is True
    assert (runtime_root / "PROJECT_REGISTRY.md").read_text(encoding="utf-8") == "runtime registry\n"
    assert (runtime_root / "01_working" / "NOW.md").read_text(encoding="utf-8") == "template now\n"


def test_perform_init_bootstraps_knowledge_base_before_sync(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "ensure_dirs", lambda paths: None)
    monkeypatch.setattr(bootstrap, "write_codex_config", lambda site: None)
    monkeypatch.setattr(bootstrap, "maybe_sync", lambda site, skip_sync: {"verify_consistency": {"returncode": 0}})
    monkeypatch.setattr(bootstrap, "maybe_install_launchagents", lambda site, install: {"installed": False, "skipped": True})
    monkeypatch.setattr(bootstrap, "maybe_install_feishu_bridge", lambda site, install: {"installed": False, "skipped": True})
    monkeypatch.setattr(bootstrap, "build_manual_actions", lambda site, payload: [])
    monkeypatch.setattr(bootstrap, "_current_lark_cli_config", lambda: {"available": False, "configured": False})
    monkeypatch.setattr(bootstrap, "_run_feishu_auth_status", lambda: {"status": {}})
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_STATUS_PATH", Path("/tmp/bootstrap-status-test.json"))
    observed: list[tuple[str, list[str]]] = []

    def fake_run_command(cmd, cwd):
        observed.append((str(cwd), list(cmd)))
        return {"returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)
    site = bootstrap.default_site_config()
    args = argparse.Namespace(skip_sync=False, install_launchagents=False, install_feishu_bridge=False)

    payload = bootstrap.perform_init(site, args)

    assert payload["knowledge_base"]["knowledge_bootstrap"]["returncode"] == 0
    assert payload["knowledge_base"]["discover_projects"]["returncode"] == 0
    assert observed[0][1] == ["python3", "ops/knowledge_intake.py", "bootstrap"]
    assert observed[1][1] == ["python3", "ops/codex_memory.py", "discover-projects"]


def test_setup_runs_acceptance_after_bootstrap(monkeypatch, capsys) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "install_python_dependencies", lambda force=False: {"returncode": 0, "installed": True})
    monkeypatch.setattr(bootstrap, "load_site_config", bootstrap.default_site_config)
    monkeypatch.setattr(bootstrap, "perform_init", lambda site, args: {"product_name": site.product_name, "ok": True})

    def fake_run_command(cmd, cwd):
        assert cmd == [bootstrap.sys.executable, "ops/accept_product.py", "run"]
        return {"returncode": 0, "stdout": '{"passed": true}', "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)

    args = argparse.Namespace(
        skip_python_deps=False,
        force_python_deps=False,
        skip_sync=False,
        install_launchagents=True,
        install_feishu_bridge=False,
        install_feishu_cli=False,
        setup_feishu_cli=False,
        create_feishu_app=False,
        skip_acceptance=False,
    )
    rc = bootstrap.cmd_setup(args)
    payload = capsys.readouterr().out

    assert rc == 0
    assert '"ok": true' in payload.lower()


def test_setup_feishu_cli_runs_unified_login_flow_by_default(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[list[str]] = []

    def fake_run_command(cmd, cwd):
        observed.append(list(cmd))
        return {"returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)
    monkeypatch.setattr(bootstrap, "command_available", lambda name: False if name == "lark-cli" else True)
    monkeypatch.setattr(bootstrap, "lark_cli_skills_installed", lambda: False)
    monkeypatch.setattr(bootstrap, "LARK_CLI_CONFIG_PATH", Path("/tmp/nonexistent-lark-config.json"))
    monkeypatch.setattr(
        bootstrap,
        "_sync_feishu_bridge_credentials",
        lambda config_init_result=None: {
            "env_path": "/tmp/feishu_bridge.env.local",
            "app_id": "cli_test",
            "app_id_synced": True,
            "app_secret_synced": True,
            "bridge_credentials_ready": True,
            "changed": True,
        },
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_feishu_auth_status",
        lambda: {"status": {"object_ops_ready": True, "coco_bridge_ready": True, "full_ready": True}},
    )

    payload = bootstrap.setup_feishu_cli(
        create_app=True,
        install=True,
        install_skills=True,
    )

    assert payload["install"]["cli"]["installed"] is True
    assert observed[0] == ["npm", "install", "-g", bootstrap.LARK_CLI_PACKAGE]
    assert observed[1] == ["npx", "skills", "add", bootstrap.LARK_CLI_SKILLS_REPO, "-y", "-g"]
    assert observed[2] == ["lark-cli", "config", "init", "--new"]
    assert observed[3] == [bootstrap.sys.executable, "ops/feishu_agent.py", "auth", "login"]
    assert len(observed) == 4
    assert payload["credentials_sync"]["bridge_credentials_ready"] is True
    assert payload["auth_login"]["returncode"] == 0
    assert payload["summary"]["full_ready"] is True
    assert payload["doctor"]["skipped"] is True


def test_install_feishu_cli_only_does_not_trigger_config_or_login(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[list[str]] = []

    def fake_run_command(cmd, cwd):
        observed.append(list(cmd))
        return {"returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)
    monkeypatch.setattr(bootstrap, "command_available", lambda name: False if name == "lark-cli" else True)
    monkeypatch.setattr(bootstrap, "lark_cli_skills_installed", lambda: False)
    monkeypatch.setattr(
        bootstrap,
        "_run_feishu_auth_status",
        lambda: {"status": {"object_ops_ready": False, "coco_bridge_ready": False, "full_ready": False}},
    )

    payload = bootstrap.install_feishu_cli_only(install_skills=True)

    assert observed == [
        ["npm", "install", "-g", bootstrap.LARK_CLI_PACKAGE],
        ["npx", "skills", "add", bootstrap.LARK_CLI_SKILLS_REPO, "-y", "-g"],
    ]
    assert payload["config_init"]["skipped"] is True
    assert payload["credentials_sync"]["skipped"] is True
    assert payload["auth_login"]["skipped"] is True


def test_setup_feishu_cli_auto_enables_feishu_in_site_config(monkeypatch, tmp_path: Path) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    site_config = tmp_path / "site.yaml"
    site_config.write_text(
        "version: 1\nsite:\n  product_name: Codex Hub\n  feishu_enabled: false\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(bootstrap, "SITE_CONFIG_PATH", site_config)

    payload = bootstrap._ensure_site_feishu_enabled()

    assert payload["feishu_enabled"] is True
    assert "feishu_enabled: true" in site_config.read_text(encoding="utf-8")


def test_setup_feishu_cli_can_skip_login_and_optionally_run_doctor(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[list[str]] = []
    existing_config = Path("/tmp/existing-lark-config.json")
    existing_config.write_text("{}", encoding="utf-8")

    def fake_run_command(cmd, cwd):
        observed.append(list(cmd))
        return {"returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)
    monkeypatch.setattr(bootstrap, "command_available", lambda name: True)
    monkeypatch.setattr(bootstrap, "lark_cli_skills_installed", lambda: True)
    monkeypatch.setattr(bootstrap, "LARK_CLI_CONFIG_PATH", existing_config)
    monkeypatch.setattr(
        bootstrap,
        "_sync_feishu_bridge_credentials",
        lambda config_init_result=None: {
            "env_path": "/tmp/feishu_bridge.env.local",
            "app_id": "cli_test",
            "app_id_synced": True,
            "app_secret_synced": False,
            "bridge_credentials_ready": False,
            "changed": True,
        },
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_feishu_auth_status",
        lambda: {"status": {"object_ops_ready": False, "coco_bridge_ready": False, "full_ready": False}},
    )

    payload = bootstrap.setup_feishu_cli(
        create_app=False,
        install=False,
        install_skills=False,
        login_user=False,
        run_doctor=True,
    )

    assert observed[0] == ["lark-cli", "doctor"]
    assert payload["auth_login"]["skipped"] is True
    assert payload["doctor"]["returncode"] == 0


def test_maybe_install_launchagents_times_out_instead_of_hanging(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[tuple[list[str], int]] = []

    def fake_run_command_with_timeout(cmd, cwd, *, timeout_seconds):
        observed.append((list(cmd), timeout_seconds))
        return {"returncode": 124, "stderr": "timed out", "timed_out": True}

    monkeypatch.setattr(bootstrap, "run_command_with_timeout", fake_run_command_with_timeout)
    site = bootstrap.default_site_config()

    payload = bootstrap.maybe_install_launchagents(site, install=True)

    assert payload["installed"] is False
    assert observed[0][0][:3] == ["python3", "ops/codex_session_watcher.py", "install-launchagent"]
    assert observed[0][1] == bootstrap.LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS
    assert payload["results"]["health_check"]["timed_out"] is True


def test_setup_with_install_feishu_cli_only_does_not_require_full_ready(monkeypatch, capsys) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "install_python_dependencies", lambda force=False: {"returncode": 0, "installed": True})
    monkeypatch.setattr(bootstrap, "load_site_config", bootstrap.default_site_config)
    monkeypatch.setattr(
        bootstrap,
        "perform_init",
        lambda site, args: {
            "product_name": site.product_name,
            "feishu_cli": {
                "install": {
                    "cli": {"returncode": 0},
                    "skills": {"returncode": 0},
                },
                "config_init": {"skipped": True},
                "credentials_sync": {"skipped": True},
                "auth_login": {"skipped": True},
                "doctor": {"skipped": True},
                "summary": {"object_ops_ready": False, "coco_bridge_ready": False, "full_ready": False},
            },
        },
    )

    def fake_run_command(cmd, cwd):
        assert cmd == [bootstrap.sys.executable, "ops/accept_product.py", "run"]
        return {"returncode": 0, "stdout": '{"passed": true}', "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)

    args = argparse.Namespace(
        skip_python_deps=False,
        force_python_deps=False,
        skip_sync=False,
        install_launchagents=True,
        install_feishu_bridge=False,
        install_feishu_cli=True,
        setup_feishu_cli=False,
        create_feishu_app=False,
        skip_acceptance=False,
    )

    rc = bootstrap.cmd_setup(args)
    payload = capsys.readouterr().out

    assert rc == 0
    assert '"ok": true' in payload.lower()


def test_build_manual_actions_guides_future_feishu_setup(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    site = bootstrap.default_site_config()
    payload = {
        "commands": {"codex": True, "lark_cli": False},
        "python_modules": {module: True for module, _package in bootstrap.PYTHON_DEPENDENCIES},
        "apps": {"codex_desktop": True, "obsidian": True},
        "feishu_setup": {},
    }

    actions = bootstrap.build_manual_actions(site, payload)

    assert any("install-feishu-cli" in action for action in actions)
    assert any("setup-feishu-cli --create-feishu-app" in action for action in actions)


def test_bootstrap_status_payload_includes_auth_and_feature_tools(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "_current_lark_cli_config", lambda: {"available": False, "configured": False})
    monkeypatch.setattr(bootstrap, "_run_feishu_auth_status", lambda: {"status": {}})
    monkeypatch.setattr(bootstrap, "codex_auth_ready", lambda: True)
    monkeypatch.setattr(
        bootstrap,
        "feature_tool_status",
        lambda: {
            "knowledge_base_pdf_ocr": {
                "label": "Knowledge Base PDF / OCR ingestion",
                "commands": {"tesseract": True, "ocrmypdf": True, "pdftoppm": True},
                "apps": {},
                "ready": True,
                "install_hint": "brew install tesseract ocrmypdf poppler",
            }
        },
    )

    payload = bootstrap.bootstrap_status_payload(bootstrap.default_site_config())

    assert payload["auth"]["codex_cli_logged_in"] is True
    assert "codex_auth_path" in payload["auth"]
    assert payload["feature_tools"]["knowledge_base_pdf_ocr"]["ready"] is True


def test_build_manual_actions_requests_codex_login_when_auth_missing(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    site = bootstrap.default_site_config()
    payload = {
        "commands": {"codex": True, "lark_cli": False},
        "auth": {"codex_cli_logged_in": False},
        "python_modules": {module: True for module, _package in bootstrap.PYTHON_DEPENDENCIES},
        "apps": {"codex_desktop": True, "obsidian": True},
        "feature_tools": {},
        "feishu_setup": {},
    }

    actions = bootstrap.build_manual_actions(site, payload)

    assert any("codex login" in action for action in actions)


def test_cmd_setup_feishu_cli_requires_full_ready(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "_ensure_site_feishu_enabled", lambda: {"changed": True, "feishu_enabled": True})
    monkeypatch.setattr(
        bootstrap,
        "load_site_config",
        lambda: bootstrap.SiteConfig(
            product_name="Codex Hub",
            workspace_root=bootstrap.WORKSPACE_ROOT,
            memory_root=bootstrap.WORKSPACE_ROOT.parent / "memory",
            operator_name="",
            timezone="Asia/Shanghai",
            launchagent_prefix="com.codexhub",
            feishu_enabled=True,
            electron_enabled=True,
        ),
    )
    monkeypatch.setattr(bootstrap, "bootstrap_status_payload", lambda site: {"manual_actions": [], "feishu_cli": {}, "feishu_setup": {}})
    monkeypatch.setattr(bootstrap, "build_manual_actions", lambda site, payload: [])
    monkeypatch.setattr(bootstrap, "_write_bootstrap_status", lambda payload: None)
    monkeypatch.setattr(
        bootstrap,
        "setup_feishu_cli",
        lambda **kwargs: {
            "install": {"skipped": True},
            "config_init": {"skipped": True},
            "credentials_sync": {"bridge_credentials_ready": False},
            "auth_login": {"skipped": True},
            "auth_status": {"status": {"object_ops_ready": True, "coco_bridge_ready": False, "full_ready": False}},
            "doctor": {"skipped": True},
            "summary": {"object_ops_ready": True, "coco_bridge_ready": False, "full_ready": False},
        },
    )
    args = argparse.Namespace(
        create_feishu_app=False,
        skip_install=True,
        skip_skills=True,
        login_lark_cli_user=False,
        skip_login=False,
        run_lark_cli_doctor=False,
    )

    rc = bootstrap.cmd_setup_feishu_cli(args)

    assert rc == 1
