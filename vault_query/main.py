"""
vq -- Query Obsidian vault frontmatter with SQL using DuckDB.

The vault is scanned for .md files; YAML frontmatter is extracted and loaded
into a DuckDB table called `notes`. Any SQL query can then be run against it.
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path

import duckdb
import yaml


def parse_frontmatter(filepath: Path) -> dict:
    """Parse YAML frontmatter from a markdown file. Returns {} if none found."""
    try:
        text = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}

    if not text.startswith("---"):
        return {}

    rest = text[3:]
    end = -1
    for delimiter in ("---", "..."):
        pos = rest.find("\n" + delimiter)
        if pos != -1:
            end = pos
            break

    if end == -1:
        return {}

    try:
        data = yaml.safe_load(rest[:end])
        return data if isinstance(data, dict) else {}
    except yaml.YAMLError:
        return {}


def scan_vault(vault_path: Path, verbose: bool = False) -> list[dict]:
    """Scan all .md files and return records with path, filename, and frontmatter."""
    md_files = sorted(vault_path.rglob("*.md"))

    if verbose:
        print(f"  Found {len(md_files)} .md files", file=sys.stderr)

    records = []
    for md_file in md_files:
        fm = parse_frontmatter(md_file)
        record: dict = {
            "path": str(md_file.relative_to(vault_path)),
            "filename": md_file.stem,
        }
        for k, v in fm.items():
            # Serialize nested structures so DuckDB can ingest them as strings
            record[k] = json.dumps(v, default=str) if isinstance(v, (list, dict)) else v
        records.append(record)

    return records


def build_table(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    """Load records into DuckDB as the 'notes' table via newline-delimited JSON."""
    if not records:
        con.execute("CREATE TABLE notes (path VARCHAR, filename VARCHAR)")
        return

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".ndjson", delete=False, encoding="utf-8"
    ) as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")
        tmp_path = f.name

    con.execute(f"CREATE TABLE notes AS SELECT * FROM read_ndjson_auto('{tmp_path}')")


def format_results(result: duckdb.DuckDBPyRelation, fmt: str) -> str:
    """Format query results as table, CSV, or JSON."""
    if fmt == "csv":
        return result.df().to_csv(index=False)
    if fmt == "json":
        return result.df().to_json(orient="records", indent=2)
    # table
    df = result.df()
    if df.empty:
        return "(no results)"
    return df.to_string(index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="vq",
        description="Query Obsidian vault frontmatter with SQL (table name: notes)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  vq BrainSync "SELECT type, count(*) FROM notes GROUP BY type ORDER BY 2 DESC"
  vq Contexta "SELECT path, description FROM notes WHERE status = 'seed'"
  vq Contexta "SELECT domain, count(*) FROM notes GROUP BY domain"
  vq BrainSync --schema
  vq Contexta --dry-run
        """,
    )
    parser.add_argument("vault", help="Vault name (in ~/vaults/) or absolute path")
    parser.add_argument("query", nargs="?", help="SQL query to run (table: notes)")
    parser.add_argument(
        "--schema", "-s", action="store_true", help="Show available columns and types"
    )
    parser.add_argument(
        "--db",
        "-d",
        metavar="FILE",
        help="Persist DuckDB to file instead of in-memory (reuse with --reuse)",
    )
    parser.add_argument(
        "--reuse",
        "-r",
        action="store_true",
        help="Reuse an existing --db file without re-scanning the vault",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["table", "csv", "json"],
        default="table",
        help="Output format (default: table)",
    )
    parser.add_argument(
        "--verbose", "-V", action="store_true", help="Show debug output on stderr"
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Scan vault and show stats without running a query",
    )

    args = parser.parse_args()

    # Validate: need something to do
    if not args.dry_run and not args.query and not args.schema:
        parser.error("provide a SQL query, or use --schema / --dry-run")

    # Resolve vault path
    vault_arg = Path(args.vault)
    vault_path = vault_arg if vault_arg.is_absolute() else Path.home() / "vaults" / args.vault

    if not args.reuse:
        if not vault_path.exists():
            print(f"Error: vault not found: {vault_path}", file=sys.stderr)
            sys.exit(1)
        if not vault_path.is_dir():
            print(f"Error: not a directory: {vault_path}", file=sys.stderr)
            sys.exit(1)

    # Scan
    if not args.reuse:
        if args.verbose:
            print(f"Scanning {vault_path} ...", file=sys.stderr)
        records = scan_vault(vault_path, verbose=args.verbose)
        notes_with_fm = sum(1 for r in records if len(r) > 2)

        if args.dry_run:
            print(f"Vault:               {vault_path}")
            print(f"Total .md files:     {len(records)}")
            print(f"With frontmatter:    {notes_with_fm}")
            print(f"Without frontmatter: {len(records) - notes_with_fm}")
            print(f"\nDone. Processed: {len(records)}, Skipped: 0")
            return

    # Connect to DuckDB
    db_path = args.db or ":memory:"
    con = duckdb.connect(db_path)

    if not args.reuse:
        build_table(con, records)
        if args.verbose:
            print(f"Loaded {len(records)} records into DuckDB", file=sys.stderr)

    # Schema mode
    if args.schema:
        result = con.execute("DESCRIBE notes")
        print(format_results(result, args.format))
        return

    # Run query
    try:
        result = con.execute(args.query)
        print(format_results(result, args.format))
    except duckdb.Error as e:
        print(f"Query error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
