"""vault-query was one of 7 repos invisible to the fleet dashboard's activity
panel because nothing in it called into local_first_common.tracking. Each
entry point (vq, vq-fix, vq-todos) now wraps its work in timed_run."""

import os
import sys
from pathlib import Path

import duckdb

from vault_query.fix import main as fix_main
from vault_query.main import main as vq_main
from vault_query.todos import main as todos_main

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
    monkeypatch.setattr(sys, "argv", ["vq", str(FIXTURES), "--dry-run"])
    vq_main()
    assert _last_run("vault-query") == ("vault-query", 3, True)


def test_vq_fix_logs_a_processing_run(monkeypatch, tmp_path):
    (tmp_path / "note.md").write_text("---\ntype: note\n---\nbody\n")
    monkeypatch.setattr(sys, "argv", ["vq-fix", str(tmp_path), "--lowercase-keys"])
    fix_main()
    tool_name, item_count, success = _last_run("vault-query")
    assert (tool_name, success) == ("vault-query", True)
    assert item_count == 1


def test_vq_todos_logs_a_processing_run(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["vq-todos", str(tmp_path)])
    todos_main()
    assert _last_run("vault-query")[0] == "vault-query"
