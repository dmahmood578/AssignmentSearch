"""
queries.py — Query patent assignment and patent text results.

Tables available in SQL:
  all_assignments  — concatenation of selected files from assignment_results/
  patent_text      — concatenation of selected files from patent_text_results/

You may query one or both tables in a single SQL statement.
"""

import os
import re
import sys
import argparse
import pandas as pd
import pandasql as ps
from datetime import datetime
from typing import Dict, List, Optional, Tuple


# ── Folder names produced by AssignmentSearch.py ──────────────────────────────
ASSIGNMENT_DIR = "assignment_results"
PATENT_TEXT_DIR = "patent_text_results"

# Timestamp pattern embedded in filenames: YYYYMMDD_HHMMSS
_TS_RE = re.compile(r"(\d{8}_\d{6})")


# ── File discovery ─────────────────────────────────────────────────────────────

def _list_result_files(folder: str) -> List[Tuple[datetime, str]]:
    """Return (datetime, filepath) pairs for all CSV/XLSX files in *folder*,
    sorted oldest-first.  Skips files whose name contains no timestamp.
    When both .csv and .xlsx exist for the same timestamp, keeps only .xlsx."""
    if not os.path.isdir(folder):
        return []
    entries = []
    for fname in os.listdir(folder):
        if not (fname.endswith(".csv") or fname.endswith(".xlsx")):
            continue
        m = _TS_RE.search(fname)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
        entries.append((ts, os.path.join(folder, fname)))
    # De-duplicate: prefer .xlsx over .csv for the same timestamp
    seen: Dict[datetime, str] = {}
    for ts, path in sorted(entries):
        ext = os.path.splitext(path)[1].lower()
        if ts not in seen or ext == ".xlsx":
            seen[ts] = path
    return sorted(seen.items())


def _load_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        return pd.read_excel(path)
    return pd.read_csv(path)


# ── Date-range filtering ───────────────────────────────────────────────────────

def _parse_dt(s: str) -> Optional[datetime]:
    """Accept flexible date/datetime strings:
    YYYYMMDD, YYYY-MM-DD, YYYYMMDD_HHMMSS, YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM"""
    s = s.strip()
    for fmt in ("%Y%m%d_%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def _filter_by_range(
    files: List[Tuple[datetime, str]],
    start: Optional[datetime],
    end: Optional[datetime],
) -> List[Tuple[datetime, str]]:
    result = []
    for ts, path in files:
        if start and ts < start:
            continue
        if end and ts > end:
            continue
        result.append((ts, path))
    return result


# ── Interactive file selection ─────────────────────────────────────────────────

def _show_files(label: str, files: List[Tuple[datetime, str]]) -> None:
    if not files:
        print(f"  (no files found in {label})")
        return
    print(f"\n  {label}:")
    for ts, path in files:
        print(f"    [{ts.strftime('%Y-%m-%d %H:%M:%S')}]  {os.path.basename(path)}")


def _ask_range(label: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Prompt the user for a start and end datetime. Enter with no input = no limit."""
    print(f"\n  Date/time range for {label} (press Enter to include all):")
    start_str = input("    From (e.g. 2026-02-26 or 20260226_130000): ").strip()
    end_str   = input("    To   (e.g. 2026-02-26 or 20260226_235959): ").strip()

    start = _parse_dt(start_str) if start_str else None
    end   = _parse_dt(end_str)   if end_str   else None

    if start_str and start is None:
        print(f"  ⚠️  Could not parse '{start_str}' — no lower bound applied.")
    if end_str and end is None:
        print(f"  ⚠️  Could not parse '{end_str}' — no upper bound applied.")
    return start, end


# ── Data loading ───────────────────────────────────────────────────────────────

def _load_table(
    folder: str,
    table_name: str,
    start: Optional[datetime],
    end: Optional[datetime],
) -> Optional[pd.DataFrame]:
    """Load and concatenate all result files within the given date range."""
    all_files = _list_result_files(folder)
    selected  = _filter_by_range(all_files, start, end)

    if not selected:
        print(f"  No files matched the date range in {folder}.")
        return None

    frames = []
    for ts, path in selected:
        try:
            df = _load_file(path)
            frames.append(df)
            print(f"  ✓ {len(df):>6} rows  ← {os.path.basename(path)}")
        except Exception as e:
            print(f"  ⚠️  Could not load {path}: {e}")

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    print(f"  → \"{table_name}\": {len(combined)} total rows from {len(frames)} file(s)")
    return combined


# ── SQL execution & output ─────────────────────────────────────────────────────

def run_query(query: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    local_ns = dict(tables)
    result = ps.sqldf(query, local_ns)
    return result if result is not None else pd.DataFrame()


def save_results(df: pd.DataFrame, base: str = "query_results") -> None:
    out_dir = "query_results"
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path  = os.path.join(out_dir, f"{base}_{ts}.csv")
    xlsx_path = os.path.join(out_dir, f"{base}_{ts}.xlsx")
    df.to_csv(csv_path, index=False)
    print(f"✓ Saved → {csv_path}")
    df.to_excel(xlsx_path, index=False)
    print(f"✓ Saved → {xlsx_path}")


# ── Example queries ────────────────────────────────────────────────────────────

_EXAMPLE_ASSIGNMENTS = """\
SELECT
    "Patent Number",
    Inventors,
    Assignees,
    "Correspondent Address",
    "Attorney Name",
    "Attorney Address"
FROM all_assignments
WHERE
    "Application Status" LIKE '%Patented Case%'
    AND ("Entity Status" = 'Micro' OR "Entity Status" = 'Small')
    AND Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'"""

_EXAMPLE_PATENT_TEXT = """\
SELECT
    "Patent Number",
    "Patent Title",
    "WIPO Field of Invention",
    "CPC Primary",
    Abstract
FROM patent_text
WHERE "WIPO Field of Invention" LIKE '%Computer technology%'"""

_EXAMPLE_JOIN = """\
SELECT
    a."Patent Number",
    a.Assignees,
    t."WIPO Field of Invention",
    t."CPC Primary",
    t.Abstract
FROM all_assignments AS a
JOIN patent_text AS t ON a."Patent Number" = t."Patent Number"
WHERE a.Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'"""


def _print_examples(tables: Dict[str, pd.DataFrame]) -> None:
    has_a = "all_assignments" in tables
    has_t = "patent_text" in tables
    print("\n" + "=" * 80)
    print("EXAMPLE QUERIES")
    print("=" * 80)
    if has_a:
        print("\n— Query all_assignments —")
        print(_EXAMPLE_ASSIGNMENTS)
    if has_t:
        print("\n— Query patent_text —")
        print(_EXAMPLE_PATENT_TEXT)
    if has_a and has_t:
        print("\n— Join both tables —")
        print(_EXAMPLE_JOIN)
    print("\n" + "=" * 80 + "\n")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query patent assignment and patent text results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python queries.py\n"
               "  python queries.py myquery.sql\n"
               "  python queries.py --query myquery.sql\n"
               "  python queries.py < myquery.sql",
    )
    parser.add_argument(
        "query_file",
        nargs="?",
        metavar="QUERY_FILE",
        help="Optional path to a .sql file containing your query. "
             "If omitted you will be prompted to type it interactively.",
    )
    parser.add_argument(
        "--query", "-q",
        dest="query_file_flag",
        metavar="QUERY_FILE",
        help="Same as passing the file as a positional argument.",
    )
    args = parser.parse_args()

    # Resolve query file from either positional or --query flag
    query_file_path = args.query_file or args.query_file_flag

    # Detect piped stdin early — all interactive prompts must be skipped
    # because stdin is the SQL content, not a keyboard
    is_piped = not sys.stdin.isatty()

    print("\n" + "=" * 80)
    print("PATENT DATA QUERY TOOL")
    print("=" * 80)

    # 1. Show available files
    a_files = _list_result_files(ASSIGNMENT_DIR)
    t_files = _list_result_files(PATENT_TEXT_DIR)

    print("\nAvailable result files:")
    _show_files(ASSIGNMENT_DIR, a_files)
    _show_files(PATENT_TEXT_DIR, t_files)

    if not a_files and not t_files:
        print("\nNo result files found. Run AssignmentSearch.py first.")
        sys.exit(1)

    # 2. Ask which tables to load and for what date range
    #    When stdin is piped, skip all prompts and load everything available.
    tables: Dict[str, pd.DataFrame] = {}

    if a_files:
        if is_piped:
            df_a = _load_table(ASSIGNMENT_DIR, "all_assignments", None, None)
        else:
            ans = input(f"\nLoad assignment_results files into \"all_assignments\"? [Y/n]: ").strip().lower()
            if ans not in ("n", "no"):
                a_start, a_end = _ask_range("assignment_results")
                df_a = _load_table(ASSIGNMENT_DIR, "all_assignments", a_start, a_end)
            else:
                df_a = None
        if df_a is not None:
            tables["all_assignments"] = df_a

    if t_files:
        if is_piped:
            df_t = _load_table(PATENT_TEXT_DIR, "patent_text", None, None)
        else:
            ans = input(f"\nLoad patent_text_results files into \"patent_text\"? [Y/n]: ").strip().lower()
            if ans not in ("n", "no"):
                t_start, t_end = _ask_range("patent_text_results")
                df_t = _load_table(PATENT_TEXT_DIR, "patent_text", t_start, t_end)
            else:
                df_t = None
        if df_t is not None:
            tables["patent_text"] = df_t

    if not tables:
        print("\nNo data loaded. Exiting.")
        sys.exit(1)

    # 4. Get query — from file arg, piped stdin, or interactive prompt
    available = ", ".join(f'"{k}"' for k in tables)

    if query_file_path:
        if not os.path.isfile(query_file_path):
            print(f"\nError: query file not found: {query_file_path}")
            sys.exit(1)
        with open(query_file_path) as fh:
            query = fh.read().strip()
        print(f"Using query from: {query_file_path}")
    elif is_piped:
        # stdin IS the SQL — all prompts were already skipped above
        query = sys.stdin.read().strip()
    else:
        _print_examples(tables)
        print(f"Enter your SQL query  (available tables: {available})")
        print("Press Ctrl+D (Mac/Linux) or Ctrl+Z then Enter (Windows) when done:")
        print("-" * 80)
        try:
            query = sys.stdin.read().strip()
        except KeyboardInterrupt:
            print("\n\nCancelled.")
            sys.exit(0)

    if not query:
        print("\nNo query entered. Exiting.")
        sys.exit(0)

    print("\nExecuting query...")
    try:
        result = run_query(query, tables)
    except Exception as e:
        print(f"\nError executing query: {e}")
        sys.exit(1)

    print(f"Query returned {len(result)} rows\n")
    if result.empty:
        print("No results.")
        sys.exit(0)

    print("Preview (first 10 rows):")
    print(result.head(10).to_string())
    print(f"\n... ({len(result)} total rows)\n")

    save_results(result)
    print("\n✓ Done.\n")


if __name__ == "__main__":
    main()
