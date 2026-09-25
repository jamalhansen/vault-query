"""
vq-fix -- Apply frontmatter normalization rules to an Obsidian vault.

Dry-runs by default. Pass --apply to write changes.
"""

import sys
from pathlib import Path
from typing import Annotated

import typer
import yaml
from local_first_common.obsidian import split_frontmatter
from local_first_common.tracking import timed_run


def apply_fixes(
    data: dict,
    rename_keys: dict[str, str],
    field_values: dict[str, dict[str, str]],
    set_fields: dict[str, str],
    lowercase_keys: bool,
) -> tuple[dict, list[str]]:
    """Return (modified_dict, list_of_change_descriptions)."""
    changes = []
    result = {}

    for key, value in data.items():
        # Explicit rename_keys takes priority; fallback to lowercase if flag set
        if key in rename_keys:
            new_key = rename_keys[key]
        elif lowercase_keys and key != key.lower():
            new_key = key.lower()
        else:
            new_key = key

        if new_key != key:
            changes.append(f"  key: {key!r} -> {new_key!r}")

        if new_key in field_values and isinstance(value, str):
            new_value = field_values[new_key].get(value, value)
            if new_value != value:
                changes.append(f"  {new_key}: {value!r} -> {new_value!r}")
            result[new_key] = new_value
        else:
            result[new_key] = value

    # Add set_fields that are absent (never overwrite existing values)
    for key, value in set_fields.items():
        if key not in result:
            changes.append(f"  +{key}: {value!r}")
            result[key] = value

    return result, changes


def process_vault(
    vault_path: Path,
    rename_keys: dict[str, str],
    field_values: dict[str, dict[str, str]],
    set_fields: dict[str, str],
    lowercase_keys: bool,
    apply: bool,
    verbose: bool,
) -> tuple[int, int, int]:
    """Walk vault, apply fixes. Returns (processed, changed, skipped)."""
    processed = changed = skipped = 0

    for md_file in sorted(vault_path.rglob("*.md")):
        try:
            text = md_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            skipped += 1
            continue
        parts = split_frontmatter(text)
        if parts is None:
            skipped += 1
            continue

        fm_yaml, body = parts
        try:
            data = yaml.safe_load(fm_yaml)
        except yaml.YAMLError:
            skipped += 1
            continue

        if not isinstance(data, dict):
            skipped += 1
            continue

        processed += 1
        new_data, changes = apply_fixes(data, rename_keys, field_values, set_fields, lowercase_keys)

        if not changes:
            continue

        changed += 1
        rel = md_file.relative_to(vault_path)
        print(f"{rel}")
        for line in changes:
            print(line)

        if apply:
            new_yaml = yaml.dump(
                new_data,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
            md_file.write_text(f"---\n{new_yaml}---\n{body}", encoding="utf-8")

    return processed, changed, skipped


app = typer.Typer(add_completion=False)


@app.command()
def main(
    vault: Annotated[str, typer.Argument(help="Vault name (in ~/vaults/) or absolute path")],
    map_file: Annotated[
        str | None, typer.Option("--map", "-m", metavar="FILE", help="YAML file defining rename_keys and field_values mappings")
    ] = None,
    lowercase_keys: Annotated[
        bool, typer.Option("--lowercase-keys", "-l", help="Lowercase all frontmatter keys (applied after --map renames)")
    ] = False,
    apply: Annotated[bool, typer.Option("--apply", "-a", help="Write changes (default is dry-run)")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-V")] = False,
) -> None:
    """Normalize frontmatter in an Obsidian vault (dry-run by default)."""
    if not map_file and not lowercase_keys:
        typer.echo("Error: provide --map, --lowercase-keys, or both", err=True)
        raise typer.Exit(2)  # a usage error, like any other bad invocation

    vault_arg = Path(vault)
    vault_path = vault_arg if vault_arg.is_absolute() else Path.home() / "vaults" / vault

    if not vault_path.is_dir():
        print(f"Error: vault not found: {vault_path}", file=sys.stderr)
        raise typer.Exit(1)

    # No LLM model involved (model=None); this just gives vq-fix a heartbeat
    # on the fleet dashboard's activity panel, which vault_query was invisible to.
    with timed_run("vault-query", None, source_location=str(vault_path)) as run:
        rename_keys: dict[str, str] = {}
        field_values: dict[str, dict[str, str]] = {}

        if map_file:
            map_path = Path(map_file)
            if not map_path.exists():
                print(f"Error: map file not found: {map_path}", file=sys.stderr)
                raise typer.Exit(1)
            with map_path.open(encoding="utf-8") as f:
                mapping = yaml.safe_load(f)
            rename_keys = mapping.get("rename_keys", {})
            field_values = mapping.get("field_values", {})
            set_fields = mapping.get("set_fields", {})
        else:
            set_fields = {}

        mode = "APPLYING" if apply else "DRY RUN"
        print(f"[{mode}] {vault_path}\n")

        processed, changed, skipped = process_vault(
            vault_path, rename_keys, field_values, set_fields,
            lowercase_keys=lowercase_keys,
            apply=apply,
            verbose=verbose,
        )
        run.item_count = processed

        print(f"\n{'Changes written' if apply else 'Would change'}: {changed} files")
        print(f"Done. Processed: {processed}, Skipped: {skipped}")
