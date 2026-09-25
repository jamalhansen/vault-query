"""vault-query was one of 7 repos invisible to the fleet dashboard's activity
panel because nothing in it called into local_first_common.tracking. Each
entry point (vq, vq-fix, vq-todos) now wraps its work in timed_run."""

import os
from pathlib import Path

import duckdb
from typer.testing import CliRunner

from vault_query.fix import app as fix_app
from vault_query.main import app as vq_app
from vault_query.todos import app as todos_app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures" / "sample_vault"


def _tracking_db():
    return duckdb.connect(os.environ["LOCAL_FIRST_TRACKING_DB"])


def _last_run(tool_name):
    return _tracking_db().execute(
        "SELECT tool_name, item_count, success FROM processing_log "
        "WHERE tool_name = ? ORDER BY created_at DESC LIMIT 1",
        [tool_name],
    ).fetchone()


def test_vq_logs_a_processing_run(monkeypatch):
    assert runner.invoke(vq_app, [str(FIXTURES), "--dry-run"]).exit_code == 0
    assert _last_run("vault-query") == ("vault-query", 3, True)


def test_vq_fix_logs_a_processing_run(monkeypatch, tmp_path):
    (tmp_path / "note.md").write_text("---\ntype: note\n---\nbody\n")
    assert runner.invoke(fix_app, [str(tmp_path), "--lowercase-keys"]).exit_code == 0
    tool_name, item_count, success = _last_run("vault-query")
    assert (tool_name, success) == ("vault-query", True)
    assert item_count == 1


def test_vq_todos_logs_a_processing_run(monkeypatch, tmp_path):
    assert runner.invoke(todos_app, [str(tmp_path)]).exit_code == 0
    assert _last_run("vault-query")[0] == "vault-query"
