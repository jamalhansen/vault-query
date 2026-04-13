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
