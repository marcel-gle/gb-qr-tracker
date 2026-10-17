from __future__ import annotations

from pathlib import Path

from campaign_pipeline.env import load_project_env


def test_load_project_env_sets_missing_and_keeps_shell_values(tmp_path: Path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("APOLLO_API_KEY=from-file\nOPENAI_API_KEY=file-openai\n", encoding="utf-8")
    monkeypatch.setenv("APOLLO_API_KEY", "placeholder")  # registers restore after the test
    monkeypatch.delenv("APOLLO_API_KEY")
    monkeypatch.setenv("OPENAI_API_KEY", "from-shell")

    assert load_project_env(env)

    import os

    assert os.environ["APOLLO_API_KEY"] == "from-file"
    assert os.environ["OPENAI_API_KEY"] == "from-shell"


def test_load_project_env_missing_file(tmp_path: Path):
    assert load_project_env(tmp_path / "missing.env") is False
