"""
vq-fix -- Apply frontmatter normalization rules to an Obsidian vault.

Dry-runs by default. Pass --apply to write changes.
"""

import argparse
import sys
from pathlib import Path

import yaml


def read_parts(filepath: Path) -> tuple[str, str] | None:
    """Split a file into (raw_frontmatter_yaml, body_after_closing_delimiter).

    Returns None if no valid frontmatter block is found.
    """
    try:
        text = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    if not text.startswith("---"):
        return None

    rest = text[3:]
    for delimiter in ("---", "..."):
        pos = rest.find("\n" + delimiter)
        if pos != -1:
            fm_yaml = rest[:pos]
            body = rest[pos + len(delimiter) + 1 :]  # everything after closing ---/...
            return fm_yaml, body

    return None


def apply_fixes(
    data: dict,
    rename_keys: dict[str, str],
    field_values: dict[str, dict[str, str]],
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

    return result, changes


def process_vault(
    vault_path: Path,
    rename_keys: dict[str, str],
    field_values: dict[str, dict[str, str]],
    lowercase_keys: bool,
    apply: bool,
    verbose: bool,
) -> tuple[int, int, int]:
    """Walk vault, apply fixes. Returns (processed, changed, skipped)."""
    processed = changed = skipped = 0

    for md_file in sorted(vault_path.rglob("*.md")):
        parts = read_parts(md_file)
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
        new_data, changes = apply_fixes(data, rename_keys, field_values, lowercase_keys)

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
            md_file.write_text(f"---\n{new_yaml}---{body}", encoding="utf-8")

    return processed, changed, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="vq-fix",
        description="Normalize frontmatter in an Obsidian vault (dry-run by default)",
    )
    parser.add_argument("vault", help="Vault name (in ~/vaults/) or absolute path")
    parser.add_argument(
        "--map", "-m", metavar="FILE",
        help="YAML file defining rename_keys and field_values mappings",
    )
    parser.add_argument(
        "--lowercase-keys", "-l", action="store_true",
        help="Lowercase all frontmatter keys (applied after --map renames)",
    )
    parser.add_argument(
        "--apply", "-a", action="store_true",
        help="Write changes (default is dry-run)",
    )
    parser.add_argument("--verbose", "-V", action="store_true")

    args = parser.parse_args()

    if not args.map and not args.lowercase_keys:
        parser.error("provide --map, --lowercase-keys, or both")

    vault_arg = Path(args.vault)
    vault_path = vault_arg if vault_arg.is_absolute() else Path.home() / "vaults" / args.vault

    if not vault_path.is_dir():
        print(f"Error: vault not found: {vault_path}", file=sys.stderr)
        sys.exit(1)

    rename_keys: dict[str, str] = {}
    field_values: dict[str, dict[str, str]] = {}

    if args.map:
        map_path = Path(args.map)
        if not map_path.exists():
            print(f"Error: map file not found: {map_path}", file=sys.stderr)
            sys.exit(1)
        with map_path.open(encoding="utf-8") as f:
            mapping = yaml.safe_load(f)
        rename_keys = mapping.get("rename_keys", {})
        field_values = mapping.get("field_values", {})

    mode = "APPLYING" if args.apply else "DRY RUN"
    print(f"[{mode}] {vault_path}\n")

    processed, changed, skipped = process_vault(
        vault_path, rename_keys, field_values,
        lowercase_keys=args.lowercase_keys,
        apply=args.apply,
        verbose=args.verbose,
    )

    print(f"\n{'Changes written' if args.apply else 'Would change'}: {changed} files")
    print(f"Done. Processed: {processed}, Skipped: {skipped}")
