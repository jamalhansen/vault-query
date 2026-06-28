"""
vq-todos -- Aggregate actionable todos across an Obsidian vault.

Todos in a vault live in three disconnected places:

  1. ops/reminders.md     -- hand-maintained markdown checkboxes
  2. ops/queue/queue.json -- pipeline task queue (status != done)
  3. note bodies          -- stray ``- [ ]`` checkboxes anywhere

This unifies all three into one list, sorted with overdue items first, and
flags anything whose date has passed. Read-only: it never writes.
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

DATE_RE = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
CHECKBOX_RE = re.compile(r"^\s*[-*] \[ \] (.+?)\s*$")
SECTION_RE = re.compile(r"^#{1,6}\s+(.+?)\s*$")

# Directories never scanned for stray checkboxes.
SKIP_DIRS = {"archive", "templates", ".obsidian", ".trash", ".git"}

# Queue statuses that are no longer actionable.
DONE_STATUSES = {"done", "complete", "completed", "archived", "cancelled"}


def parse_reminders(vault_path: Path, verbose: bool = False) -> list[dict]:
    """Parse ``- [ ]`` checkboxes from ops/reminders.md, tracking section headers."""
    path = vault_path / "ops" / "reminders.md"
    if not path.exists():
        if verbose:
            print(f"  no reminders file at {path}", file=sys.stderr)
        return []

    todos: list[dict] = []
    section = None
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        header = SECTION_RE.match(line)
        if header:
            section = header.group(1)
            continue
        m = CHECKBOX_RE.match(line)
        if not m:
            continue
        body = m.group(1).strip()
        item_date, tag, text = _split_prefix(body)
        todos.append(
            {
                "source": "reminders",
                "date": item_date,
                "tag": tag,
                "text": text,
                "group": section,
                "location": f"ops/reminders.md:{lineno}",
            }
        )
    return todos


def _split_prefix(body: str) -> tuple[str | None, str | None, str]:
    """Split a reminder body's leading ``date:`` or ``tag:`` token from its text."""
    if ":" in body:
        prefix, rest = body.split(":", 1)
        prefix, rest = prefix.strip(), rest.strip()
        if ISO_DATE.match(prefix):
            return prefix, None, rest
        # Short, label-like prefix (e.g. "active", "~next-week", "in-progress (Jordan)").
        if len(prefix) <= 30 and "[[" not in prefix:
            return None, prefix, rest
    return None, None, body


def parse_queue(vault_path: Path, verbose: bool = False) -> list[dict]:
    """Parse open tasks from ops/queue/queue.json (status not in DONE_STATUSES)."""
    path = vault_path / "ops" / "queue" / "queue.json"
    if not path.exists():
        if verbose:
            print(f"  no queue file at {path}", file=sys.stderr)
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"Warning: could not parse {path}: {exc}", file=sys.stderr)
        return []

    todos: list[dict] = []
    for task in data.get("tasks", []):
        status = str(task.get("status", "")).lower()
        if status in DONE_STATUSES:
            continue
        item_date = task.get("due") or _date_part(task.get("created"))
        text = task.get("target") or task.get("id") or "(untitled task)"
        phase = task.get("current_phase")
        todos.append(
            {
                "source": "queue",
                "date": item_date,
                "tag": status or None,
                "text": f"{text} [{phase}]" if phase else text,
                "group": task.get("type"),
                "location": f"queue:{task.get('id', '?')}",
            }
        )
    return todos


def _date_part(value: object) -> str | None:
    """Pull a YYYY-MM-DD prefix out of an ISO timestamp, if present."""
    if not isinstance(value, str):
        return None
    m = DATE_RE.search(value)
    return m.group(1) if m else None


def parse_notes(
    vault_path: Path, include_archive: bool = False, verbose: bool = False
) -> list[dict]:
    """Find stray ``- [ ]`` checkboxes in note bodies (excludes reminders.md)."""
    skip = SKIP_DIRS - {"archive"} if include_archive else SKIP_DIRS
    reminders = vault_path / "ops" / "reminders.md"

    todos: list[dict] = []
    for md in sorted(vault_path.rglob("*.md")):
        if md == reminders or any(part in skip for part in md.parts):
            continue
        try:
            lines = md.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for lineno, line in enumerate(lines, 1):
            m = CHECKBOX_RE.match(line)
            if not m:
                continue
            body = m.group(1).strip()
            found = DATE_RE.search(body)
            todos.append(
                {
                    "source": "notes",
                    "date": found.group(1) if found else None,
                    "tag": None,
                    "text": body,
                    "group": md.stem,
                    "location": f"{md.relative_to(vault_path)}:{lineno}",
                }
            )
    if verbose:
        print(f"  {len(todos)} stray checkboxes in notes", file=sys.stderr)
    return todos


def collect(
    vault_path: Path, sources: set[str], include_archive: bool, verbose: bool
) -> list[dict]:
    todos: list[dict] = []
    if "reminders" in sources:
        todos += parse_reminders(vault_path, verbose)
    if "queue" in sources:
        todos += parse_queue(vault_path, verbose)
    if "notes" in sources:
        todos += parse_notes(vault_path, include_archive, verbose)
    return todos


def mark_overdue(todos: list[dict], today: date) -> None:
    for t in todos:
        d = t.get("date")
        t["overdue"] = bool(d and ISO_DATE.match(d) and d < today.isoformat())


def sort_key(t: dict) -> tuple:
    """Overdue first, then dated ascending, then undated last."""
    d = t.get("date")
    return (not t.get("overdue"), d is None, d or "9999-99-99")


def render_text(todos: list[dict]) -> str:
    if not todos:
        return "No open todos found."
    out: list[str] = []
    by_source: dict[str, list[dict]] = {}
    for t in todos:
        by_source.setdefault(t["source"], []).append(t)

    labels = {"reminders": "REMINDERS", "queue": "QUEUE", "notes": "IN-NOTE CHECKBOXES"}
    for source in ("reminders", "queue", "notes"):
        items = by_source.get(source)
        if not items:
            continue
        out.append(f"\n=== {labels[source]} ({len(items)}) ===")
        last_group = object()
        for t in items:
            if t.get("group") != last_group:
                last_group = t.get("group")
                if last_group:
                    out.append(f"  # {last_group}")
            flag = "OVERDUE " if t.get("overdue") else ""
            when = t["date"] or t.get("tag") or "—"
            out.append(f"  [{flag}{when}] {t['text']}")
            out.append(f"      ↳ {t['location']}")
    return "\n".join(out)


def render_csv(todos: list[dict]) -> str:
    buf = io.StringIO()
    cols = ["source", "date", "overdue", "tag", "group", "text", "location"]
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(todos)
    return buf.getvalue().rstrip("\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="vq-todos",
        description="Aggregate actionable todos across a vault (reminders + queue + note checkboxes)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  vq-todos                       # all sources in KeySix, overdue first
  vq-todos --overdue             # only items past their date
  vq-todos -s reminders          # just ops/reminders.md
  vq-todos BrainSync -f json     # another vault, JSON output
        """,
    )
    parser.add_argument(
        "vault",
        nargs="?",
        default=os.environ.get("VQ_TODOS_VAULT", "KeySix"),
        help="Vault name (in ~/vaults/) or absolute path (default: KeySix or $VQ_TODOS_VAULT)",
    )
    parser.add_argument(
        "--source",
        "-s",
        choices=["all", "reminders", "queue", "notes"],
        default="all",
        help="Which source(s) to pull from (default: all)",
    )
    parser.add_argument(
        "--overdue", "-o", action="store_true", help="Show only overdue items"
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--include-archive",
        "-a",
        action="store_true",
        help="Include checkboxes under archive/ (skipped by default)",
    )
    parser.add_argument(
        "--today",
        metavar="YYYY-MM-DD",
        help="Override today's date for overdue calculation (testing)",
    )
    parser.add_argument(
        "--verbose", "-V", action="store_true", help="Show debug output on stderr"
    )
    args = parser.parse_args()

    vault_arg = Path(args.vault)
    vault_path = vault_arg if vault_arg.is_absolute() else Path.home() / "vaults" / args.vault
    if not vault_path.is_dir():
        print(f"Error: vault not found: {vault_path}", file=sys.stderr)
        sys.exit(1)

    if args.today:
        if not ISO_DATE.match(args.today):
            print("Error: --today must be YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
        today = date.fromisoformat(args.today)
    else:
        today = date.today()

    sources = {"reminders", "queue", "notes"} if args.source == "all" else {args.source}
    todos = collect(vault_path, sources, args.include_archive, args.verbose)
    mark_overdue(todos, today)
    if args.overdue:
        todos = [t for t in todos if t["overdue"]]
    todos.sort(key=sort_key)

    if args.format == "json":
        print(json.dumps(todos, indent=2))
    elif args.format == "csv":
        print(render_csv(todos))
    else:
        print(render_text(todos))

    overdue = sum(1 for t in todos if t.get("overdue"))
    dated = sum(1 for t in todos if t.get("date") and not t.get("overdue"))
    undated = len(todos) - overdue - dated
    print(
        f"\nDone. Total: {len(todos)} "
        f"(overdue {overdue}, dated {dated}, undated {undated})",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
