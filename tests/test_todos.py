import json
from datetime import date

from vault_query.todos import (
    _parse_obsidian_fields,
    mark_overdue,
    parse_notes,
    parse_queue,
    parse_reminders,
    render_text,
    sort_key,
)

REMINDERS = """\
# Reminders

## Imminent

- [ ] 2026-06-29: Launch day
- [ ] 2026-01-01: Overdue thing

## Active

- [ ] active: No date, just a tag
- [ ] in-progress (Jordan): Smoke tests
- [x] done: should be ignored
"""

QUEUE = {
    "tasks": [
        {"id": "t1", "status": "pending", "target": "Note A", "current_phase": "connect",
         "created": "2026-05-03T20:00:00Z", "type": "capture"},
        {"id": "t2", "status": "done", "target": "Note B"},
    ]
}


def _vault(tmp_path):
    (tmp_path / "ops" / "queue").mkdir(parents=True)
    (tmp_path / "ops" / "reminders.md").write_text(REMINDERS)
    (tmp_path / "ops" / "queue" / "queue.json").write_text(json.dumps(QUEUE))
    return tmp_path


class TestParseReminders:
    def test_extracts_open_checkboxes_only(self, tmp_path):
        todos = parse_reminders(_vault(tmp_path))
        assert len(todos) == 4  # [x] done is excluded

    def test_parses_date_prefix(self, tmp_path):
        todos = parse_reminders(_vault(tmp_path))
        launch = next(t for t in todos if "Launch day" in t["text"])
        assert launch["date"] == "2026-06-29"
        assert launch["tag"] is None

    def test_parses_tag_prefix(self, tmp_path):
        todos = parse_reminders(_vault(tmp_path))
        tagged = next(t for t in todos if "No date" in t["text"])
        assert tagged["date"] is None
        assert tagged["tag"] == "active"

    def test_tracks_section_as_group(self, tmp_path):
        todos = parse_reminders(_vault(tmp_path))
        launch = next(t for t in todos if "Launch day" in t["text"])
        assert launch["group"] == "Imminent"

    def test_missing_file_returns_empty(self, tmp_path):
        assert parse_reminders(tmp_path) == []


class TestParseQueue:
    def test_excludes_done_tasks(self, tmp_path):
        todos = parse_queue(_vault(tmp_path))
        assert len(todos) == 1
        assert "Note A" in todos[0]["text"]

    def test_includes_phase_and_date(self, tmp_path):
        todos = parse_queue(_vault(tmp_path))
        assert "[connect]" in todos[0]["text"]
        assert todos[0]["date"] == "2026-05-03"

    def test_missing_file_returns_empty(self, tmp_path):
        assert parse_queue(tmp_path) == []


class TestParseNotes:
    def test_finds_checkboxes_and_skips_archive(self, tmp_path):
        (tmp_path / "note.md").write_text("- [ ] do this on 2026-07-01\n- [x] done\n")
        (tmp_path / "archive").mkdir()
        (tmp_path / "archive" / "old.md").write_text("- [ ] archived task\n")
        todos = parse_notes(tmp_path)
        assert len(todos) == 1
        assert todos[0]["date"] == "2026-07-01"

    def test_include_archive_flag(self, tmp_path):
        (tmp_path / "archive").mkdir()
        (tmp_path / "archive" / "old.md").write_text("- [ ] archived task\n")
        assert len(parse_notes(tmp_path, include_archive=True)) == 1

    def test_skips_reminders_file(self, tmp_path):
        _vault(tmp_path)
        # reminders.md checkboxes must not be double-counted as notes
        assert parse_notes(tmp_path) == []


class TestObsidianFields:
    def test_due_date(self):
        f = _parse_obsidian_fields("Buy milk 📅 2026-07-01")
        assert f["date"] == "2026-07-01"

    def test_start_date(self):
        f = _parse_obsidian_fields("Write post 🛫 2026-06-30 📅 2026-07-05")
        assert f["start"] == "2026-06-30"
        assert f["date"] == "2026-07-05"

    def test_scheduled_date(self):
        f = _parse_obsidian_fields("Review PR ⏳ 2026-07-03")
        assert f["scheduled"] == "2026-07-03"

    def test_recurrence(self):
        f = _parse_obsidian_fields("Weekly standup 🔁 every week 📅 2026-07-07")
        assert "every week" in f["recurrence"]

    def test_priority_high(self):
        f = _parse_obsidian_fields("Critical fix 🔺 do it now")
        assert f["priority"] == "highest"

    def test_priority_low(self):
        f = _parse_obsidian_fields("Nice to have 🔽 someday")
        assert f["priority"] == "low"

    def test_prefers_due_emoji_over_body_date(self, tmp_path):
        (tmp_path / "note.md").write_text(
            "- [ ] Do the thing (2026-06-01) #tag 📅 2026-07-15\n"
        )
        todos = parse_notes(tmp_path)
        assert todos[0]["date"] == "2026-07-15"


class TestNotStarted:
    def test_not_started_flagged_for_future_start(self):
        todos = [{"start": "2026-08-01"}, {"start": "2026-01-01"}, {"start": None}]
        mark_overdue(todos, date(2026, 6, 28))
        assert todos[0]["not_started"] is True
        assert todos[1]["not_started"] is False
        assert todos[2]["not_started"] is False


class TestByFile:
    def test_by_file_groups_on_location(self, tmp_path):
        (tmp_path / "a.md").write_text("- [ ] task one\n- [ ] task two\n")
        (tmp_path / "b.md").write_text("- [ ] task three\n")
        todos = parse_notes(tmp_path)
        mark_overdue(todos, date(2026, 6, 28))
        output = render_text(todos, by_file=True)
        assert "a.md" in output
        assert "b.md" in output
        assert output.index("a.md") < output.index("b.md")


class TestOverdueAndSort:
    def test_mark_overdue(self):
        todos = [{"date": "2026-01-01"}, {"date": "2026-12-31"}, {"date": None}]
        mark_overdue(todos, date(2026, 6, 27))
        assert todos[0]["overdue"] is True
        assert todos[1]["overdue"] is False
        assert todos[2]["overdue"] is False

    def test_sort_overdue_first_undated_last(self):
        todos = [
            {"date": None, "overdue": False},
            {"date": "2026-01-01", "overdue": True},
            {"date": "2026-12-31", "overdue": False},
        ]
        todos.sort(key=sort_key)
        assert todos[0]["overdue"] is True
        assert todos[-1]["date"] is None
