from __future__ import annotations

import argparse
import importlib
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


def test_maybe_install_launchagents_uses_poll_interval_for_watcher(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    observed: list[list[str]] = []

    def fake_run_command(cmd, cwd):
        observed.append(list(cmd))
        return {"returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(bootstrap, "run_command", fake_run_command)
    site = bootstrap.default_site_config()

    payload = bootstrap.maybe_install_launchagents(site, install=True)

    assert payload["installed"] is True
    assert observed[0][:4] == ["python3", "ops/codex_session_watcher.py", "install-launchagent", "--poll-interval"]
    assert observed[0][4] == "300"
    assert observed[-1][:3] == ["python3", "ops/knowledge_intake.py", "install-launchagent"]


def test_perform_init_bootstraps_knowledge_base_before_sync(monkeypatch) -> None:
    from ops import bootstrap_workspace_hub as bootstrap_module

    bootstrap = importlib.reload(bootstrap_module)
    monkeypatch.setattr(bootstrap, "ensure_dirs", lambda paths: None)
    monkeypatch.setattr(bootstrap, "write_codex_config", lambda site: None)
    monkeypatch.setattr(bootstrap, "maybe_sync", lambda site, skip_sync: {"verify_consistency": {"returncode": 0}})
    monkeypatch.setattr(bootstrap, "maybe_install_launchagents", lambda site, install: {"installed": False, "skipped": True})
    monkeypatch.setattr(bootstrap, "maybe_install_feishu_bridge", lambda site, install: {"installed": False, "skipped": True})
    monkeypatch.setattr(bootstrap, "build_manual_actions", lambda site, payload: [])
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


def test_setup_feishu_cli_calls_install_and_config_only_by_default(monkeypatch) -> None:
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

    payload = bootstrap.setup_feishu_cli(
        create_app=True,
        install=True,
        install_skills=True,
    )

    assert payload["install"]["cli"]["installed"] is True
    assert observed[0] == ["npm", "install", "-g", bootstrap.LARK_CLI_PACKAGE]
    assert observed[1] == ["npx", "skills", "add", bootstrap.LARK_CLI_SKILLS_REPO, "-y", "-g"]
    assert observed[2] == ["lark-cli", "config", "init", "--new"]
    assert len(observed) == 3
    assert payload["auth_login"]["skipped"] is True
    assert payload["doctor"]["skipped"] is True


def test_setup_feishu_cli_can_optionally_run_lark_cli_login_and_doctor(monkeypatch) -> None:
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

    payload = bootstrap.setup_feishu_cli(
        create_app=False,
        install=False,
        install_skills=False,
        login_user=True,
        run_doctor=True,
    )

    assert observed[0] == ["lark-cli", "auth", "login", "--domain", bootstrap.DEFAULT_FEISHU_CLI_DOMAINS]
    assert observed[1] == ["lark-cli", "doctor"]
    assert payload["auth_login"]["returncode"] == 0
    assert payload["doctor"]["returncode"] == 0
