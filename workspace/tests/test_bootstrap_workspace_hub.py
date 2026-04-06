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
    monkeypatch.setattr(bootstrap, "maybe_auto_install_macos_features", lambda site, install: {"installed": True, "results": {}, "features": []})
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
    args = argparse.Namespace(skip_sync=False, install_launchagents=False, install_feishu_bridge=False, skip_macos_auto_install=False)

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
        skip_macos_auto_install=False,
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


def test_detect_system_package_manager_prefers_priority_order(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(
        bootstrap,
        "command_available",
        lambda name: name in {"brew", "apt-get", "winget", "choco"},
    )

    assert bootstrap.detect_system_package_manager() == "brew"


def test_build_system_install_command_supports_supported_managers() -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)

    assert bootstrap.build_system_install_command("knowledge_base_pdf_ocr", manager="brew") == [
        "brew",
        "install",
        "tesseract",
        "ocrmypdf",
        "poppler",
    ]
    assert bootstrap.build_system_install_command("knowledge_base_pdf_ocr", manager="apt") == [
        "sudo",
        "apt-get",
        "install",
        "-y",
        "tesseract-ocr",
        "ocrmypdf",
        "poppler-utils",
    ]
    assert bootstrap.build_system_install_command("knowledge_base_pdf_ocr", manager="winget") == [
        "winget",
        "install",
        "--id",
        "UB-Mannheim.TesseractOCR",
        "--exact",
        "--accept-package-agreements",
        "--accept-source-agreements",
        "&&",
        "winget",
        "install",
        "--id",
        "OCRmyPDF.OCRMYPDF",
        "--exact",
        "--accept-package-agreements",
        "--accept-source-agreements",
        "&&",
        "winget",
        "install",
        "--id",
        "oschwartz10612.Poppler",
        "--exact",
        "--accept-package-agreements",
        "--accept-source-agreements",
    ]
    assert bootstrap.build_system_install_command("knowledge_base_pdf_ocr", manager="choco") == [
        "choco",
        "install",
        "-y",
        "tesseract",
        "ocrmypdf",
        "poppler",
    ]


def test_feature_doctor_uses_persisted_bootstrap_status_for_knowledge_base(monkeypatch, tmp_path: Path) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    status_path = tmp_path / "bootstrap-status.json"
    status_path.write_text(
        json.dumps(
            {
                "knowledge_base": {
                    "knowledge_bootstrap": {"returncode": 0},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_STATUS_PATH", status_path)
    monkeypatch.setattr(bootstrap, "_current_lark_cli_config", lambda: {"available": False, "configured": False})
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
            },
            "opencli_browser": {
                "label": "OpenCLI browser execution",
                "commands": {},
                "apps": {"Google Chrome": True},
                "ready": True,
                "install_hint": "install Google Chrome",
            },
        },
    )
    monkeypatch.setattr(bootstrap, "command_available", lambda name: name == "python3")

    payload = bootstrap.feature_doctor("knowledge-base", bootstrap.default_site_config())

    assert payload["ready"] is True
    assert [check["name"] for check in payload["checks"]] == ["ocr_tools_ready", "knowledge_bootstrap"]
    assert all(check["ok"] for check in payload["checks"])


def test_install_feature_knowledge_base_dry_run_uses_detected_manager(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "detect_system_package_manager", lambda: "apt")
    monkeypatch.setattr(bootstrap, "is_macos", lambda: False)

    payload = bootstrap.install_feature("knowledge-base", bootstrap.default_site_config(), dry_run=True)

    assert payload["ok"] is True
    assert payload["system_result"]["manager"] == "apt"
    assert payload["system_result"]["command"] == [
        "sudo",
        "apt-get",
        "install",
        "-y",
        "tesseract-ocr",
        "ocrmypdf",
        "poppler-utils",
    ]


def test_install_feature_electron_dry_run_returns_app_workspace(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "detect_system_package_manager", lambda: "brew")
    monkeypatch.setattr(bootstrap, "install_system_group", lambda group_id, *, manager="", dry_run=False: {"ok": True, "group_id": group_id, "manager": manager or "brew", "dry_run": dry_run})

    payload = bootstrap.install_feature("electron", bootstrap.default_site_config(), dry_run=True)

    assert payload["ok"] is True
    assert payload["command"] == ["npm", "install"]
    assert payload["cwd"] == str(bootstrap.WORKSPACE_ROOT / "apps" / "electron-console")
    assert payload["runtime_result"]["group_id"] == "node_runtime"


def test_install_feature_opencli_dry_run_installs_runtime_browser_and_cli(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "detect_system_package_manager", lambda: "brew")
    monkeypatch.setattr(bootstrap, "install_system_group", lambda group_id, *, manager="", dry_run=False: {"ok": True, "group_id": group_id, "manager": manager or "brew", "dry_run": dry_run})
    monkeypatch.setattr(bootstrap, "install_npm_global_package", lambda package_name, *, dry_run=False: {"ok": True, "package": package_name, "dry_run": dry_run, "command": ["npm", "install", "-g", package_name]})

    payload = bootstrap.install_feature("opencli", bootstrap.default_site_config(), dry_run=True)

    assert payload["ok"] is True
    assert payload["runtime_result"]["group_id"] == "node_runtime"
    assert payload["browser_result"]["group_id"] == "opencli_browser"
    assert payload["cli_result"]["package"] == bootstrap.OPENCLI_NPM_PACKAGE


def test_install_homebrew_dry_run_reports_official_command(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "is_macos", lambda: True)
    monkeypatch.setattr(bootstrap, "command_available", lambda name: False)

    payload = bootstrap.install_homebrew(dry_run=True)

    assert payload["ok"] is True
    assert payload["manager"] == "brew"
    assert payload["command"] == ["/bin/bash", "-lc", bootstrap.HOMEBREW_INSTALL_SHELL]


def test_install_system_group_skips_node_runtime_when_commands_exist(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "node_runtime_ready", lambda: True)

    payload = bootstrap.install_system_group("node_runtime", manager="brew", dry_run=False)

    assert payload["ok"] is True
    assert payload["skipped"] is True
    assert payload["group_id"] == "node_runtime"


def test_maybe_auto_install_macos_features_runs_default_features(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "is_macos", lambda: True)
    observed: list[str] = []

    def fake_install_feature(feature, site, *, dry_run=False):
        observed.append(feature)
        return {"feature": feature, "ok": True}

    monkeypatch.setattr(bootstrap, "install_feature", fake_install_feature)

    payload = bootstrap.maybe_auto_install_macos_features(bootstrap.default_site_config(), install=True)

    assert payload["installed"] is True
    assert observed == list(bootstrap.MACOS_AUTO_INSTALL_FEATURES)


def test_build_parser_exposes_feature_doctor_and_installers() -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    parser = bootstrap.build_parser()

    doctor_args = parser.parse_args(["doctor-feature", "--feature", "knowledge-base"])
    install_feature_args = parser.parse_args(["install-feature", "--feature", "electron", "--dry-run"])
    install_group_args = parser.parse_args(["install-system-deps", "--group", "opencli_browser", "--dry-run"])

    assert doctor_args.command == "doctor-feature"
    assert install_feature_args.command == "install-feature"
    assert install_feature_args.dry_run is True
    assert install_group_args.command == "install-system-deps"
    assert install_group_args.group == "opencli_browser"


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
