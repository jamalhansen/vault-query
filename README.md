# vq -- Vault Query

Query Obsidian vault frontmatter with SQL using DuckDB.

## Installation

```bash
cd ~/projects/vault-query
uv sync
```

The `vq` command is then available via `uv run vq` or by activating the venv.

## Usage

```bash
# Vault name resolves to ~/vaults/<name>
vq <vault> "<sql>"        # run a query (table name: notes)
vq <vault> --schema       # show available columns
vq <vault> --dry-run      # scan stats without querying

# Output formats
vq <vault> -f csv "<sql>"
vq <vault> -f json "<sql>"

# Persist DB to avoid re-scanning on repeated queries
vq <vault> --db /tmp/vault.duckdb "<sql>"
vq <vault> --db /tmp/vault.duckdb --reuse "<sql>"
```

## Examples

```bash
vq Contexta "SELECT domain, status, count(*) FROM notes GROUP BY domain, status ORDER BY domain, status"
vq BrainSync "SELECT path, description FROM notes WHERE status = 'seed'"
vq Contexta "SELECT * FROM notes WHERE type = 'map'"
```

## Notes table

Every `.md` file in the vault gets a row. Columns are derived from YAML frontmatter dynamically -- use `--schema` to see what's available for a given vault. All files appear in the table; those without frontmatter have only `path` and `filename`.

Standard Contexta/BrainSync columns: `path`, `filename`, `type`, `domain`, `status`, `description`, `created`, `tags`.

## vq-todos -- Todo aggregator

`vq` queries frontmatter, but todos live elsewhere: hand-maintained checkboxes in
`ops/reminders.md`, pipeline tasks in `ops/queue/queue.json`, and stray `- [ ]`
checkboxes in note bodies. `vq-todos` unions all three into one list, sorts overdue
items first, and flags anything past its date. Read-only.

```bash
vq-todos                    # all sources in KeySix (default vault), overdue first
vq-todos --overdue          # only items whose date has passed
vq-todos --not-started      # items whose 🛫 start date is in the future
vq-todos -s notes --by-file # note checkboxes grouped by source file (for cleanup)
vq-todos -s reminders       # one source: reminders | queue | notes | all
vq-todos -a                 # include checkboxes under archive/ (skipped by default)
vq-todos BrainSync -f json  # another vault; -f text | json | csv
vq-todos --today 2026-06-29 # override "today" for overdue calculation
```

Default vault is `KeySix` (override with `$VQ_TODOS_VAULT` or a positional arg).
A reminder line's leading token is read as a date (`2026-06-29:`) or a status tag
(`active:`, `in-progress (Jordan):`). The summary line goes to stderr so piped
output stays clean.

**Obsidian Tasks emoji fields:** note checkboxes are also parsed for the
[Obsidian Tasks](https://publish.obsidian.md/tasks/) plugin's emoji syntax --
📅 due, 🛫 start, ⏳ scheduled, 🔁 recurrence, and priority (⏫🔺🔼🔽➕). A plain
ISO date in the checkbox text is still picked up as a fallback when there's no 📅.
