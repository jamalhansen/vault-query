from pathlib import Path

import duckdb

from vault_query.main import build_table, parse_frontmatter, scan_vault

FIXTURES = Path(__file__).parent / "fixtures" / "sample_vault"


class TestParseFrontmatter:
    def test_parses_valid_frontmatter(self):
        result = parse_frontmatter(FIXTURES / "active_note.md")
        assert result["type"] == "note"
        assert result["domain"] == "ai-tools"
        assert result["status"] == "active"

    def test_returns_empty_for_no_frontmatter(self):
        result = parse_frontmatter(FIXTURES / "no_frontmatter.md")
        assert result == {}

    def test_returns_empty_for_missing_file(self, tmp_path):
        result = parse_frontmatter(tmp_path / "nonexistent.md")
        assert result == {}

    def test_list_field_serialized_to_string(self):
        result = parse_frontmatter(FIXTURES / "active_note.md")
        # tags is a list in YAML; parse_frontmatter returns it as-is
        assert isinstance(result["tags"], list)

    def test_parses_date_field(self):
        result = parse_frontmatter(FIXTURES / "active_note.md")
        # pyyaml returns dates as datetime.date objects
        assert str(result["created"]) == "2026-03-15"


class TestScanVault:
    def test_finds_all_md_files(self):
        records = scan_vault(FIXTURES)
        assert len(records) == 3

    def test_all_records_have_path_and_filename(self):
        records = scan_vault(FIXTURES)
        for r in records:
            assert "path" in r
            assert "filename" in r

    def test_no_frontmatter_note_has_only_path_and_filename(self):
        records = scan_vault(FIXTURES)
        bare = next(r for r in records if r["filename"] == "no_frontmatter")
        assert set(bare.keys()) == {"path", "filename"}

    def test_list_fields_serialized_to_json_string(self):
        records = scan_vault(FIXTURES)
        note = next(r for r in records if r["filename"] == "active_note")
        assert isinstance(note["tags"], str)
        assert '"llm"' in note["tags"]


class TestBuildTable:
    def test_table_created_with_records(self):
        records = scan_vault(FIXTURES)
        con = duckdb.connect(":memory:")
        build_table(con, records)
        count = con.execute("SELECT count(*) FROM notes").fetchone()[0]
        assert count == 3

    def test_empty_records_creates_minimal_table(self):
        con = duckdb.connect(":memory:")
        build_table(con, [])
        result = con.execute("SELECT * FROM notes").fetchall()
        assert result == []

    def test_query_by_status(self):
        records = scan_vault(FIXTURES)
        con = duckdb.connect(":memory:")
        build_table(con, records)
        rows = con.execute("SELECT filename FROM notes WHERE status = 'seed'").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "seed_note"

    def test_query_count_by_domain(self):
        records = scan_vault(FIXTURES)
        con = duckdb.connect(":memory:")
        build_table(con, records)
        rows = con.execute(
            "SELECT domain, count(*) as n FROM notes WHERE domain IS NOT NULL GROUP BY domain ORDER BY domain"
        ).fetchall()
        domains = {r[0]: r[1] for r in rows}
        assert domains["ai-tools"] == 1
        assert domains["platform"] == 1
