from vault_query.fix import process_vault


def run(vault, apply=True):
    return process_vault(vault, {}, {}, {}, lowercase_keys=True, apply=apply, verbose=False)


def test_apply_rewrites_frontmatter_and_keeps_body_byte_for_byte(tmp_path):
    body = "\n# Title\n\nIntro\n\n---\n\nAfter a horizontal rule\n"
    note = tmp_path / "n.md"
    note.write_text(f"---\nStatus: draft\n---\n{body}", encoding="utf-8")

    assert run(tmp_path) == (1, 1, 0)
    assert note.read_text(encoding="utf-8") == f"---\nstatus: draft\n---\n{body}"


def test_dry_run_does_not_write(tmp_path):
    note = tmp_path / "n.md"
    original = "---\nStatus: draft\n---\nBody\n"
    note.write_text(original, encoding="utf-8")

    assert run(tmp_path, apply=False) == (1, 1, 0)
    assert note.read_text(encoding="utf-8") == original


def test_skips_notes_without_valid_frontmatter(tmp_path):
    (tmp_path / "plain.md").write_text("# No frontmatter\n", encoding="utf-8")
    (tmp_path / "bad.md").write_text("---\na: [unclosed\n---\nBody\n", encoding="utf-8")
    (tmp_path / "list.md").write_text("---\n- a\n---\nBody\n", encoding="utf-8")

    assert run(tmp_path) == (0, 0, 3)
