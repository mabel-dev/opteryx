#!/usr/bin/env python3
"""
Basic LOC counter for this repo.
Counts non-blank, non-comment lines for file extensions typically used in this repo:
- .py .pyx .c .cpp .cc .cxx .h .hpp

Comments are identified only if the line starts (after whitespace) with `#` or `//`.
This is intentionally simple / fast.

Usage:
    python dev/count_loc_basic.py [--root ROOT] [--exclude DIR1,DIR2] [--ext py,pyx,c,cpp,h] [--top N]

The script prints a total and per-language breakdown.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Set
from typing import Tuple

DEFAULT_EXTS = ["py", "pyx", "c", "cpp", "cc", "cxx", "h", "hpp"]
DEFAULT_EXCLUDES = {"build", "temp", "third_party", "dev", "dist", "scratch"}


def parse_args():
    p = argparse.ArgumentParser(description="Basic LOC counter (non-blank, non-comment lines)")
    p.add_argument("--root", default=".", help="Root directory to scan")
    p.add_argument(
        "--exclude",
        default=','.join(sorted(DEFAULT_EXCLUDES)),
        help=f"Comma-separated list of directory names to exclude. Default: {','.join(sorted(DEFAULT_EXCLUDES))}",
    )
    p.add_argument(
        "--ext",
        default=','.join(DEFAULT_EXTS),
        help=f"Comma-separated extensions to include (no leading dot). Default: {','.join(DEFAULT_EXTS)}",
    )
    p.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N files by LOC (default 10)",
    )
    p.add_argument(
        "--per-file",
        action="store_true",
        help="Show counts per file in addition to summary",
    )
    return p.parse_args()


def should_skip(path: Path, exclude_parts: Set[str]) -> bool:
    """Return True if any component of `path` is in exclude_parts."""
    return any(part in exclude_parts for part in path.parts)


def find_files(root: Path, exts: Set[str], exclude_parts: Set[str]) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file():
            if should_skip(path, exclude_parts):
                continue
            if path.suffix:
                suf = path.suffix[1:]
                if suf in exts:
                    yield path


def count_file(path: Path) -> int:
    cnt = 0
    try:
        with path.open("r", errors="replace") as fh:
            for line in fh:
                if not line.strip():
                    continue
                s = line.lstrip()
                if s.startswith("#") or s.startswith("//"):
                    continue
                cnt += 1
    except (OSError, UnicodeDecodeError):
        # If we can't read a file for whatever reason, just skip it and return 0
        return 0
    return cnt


def group_by_ext(path: Path) -> str:
    suf = path.suffix[1:]
    if suf in ("py", "pyx"):
        return "Python/Cython"
    if suf in ("c",):
        return "C"
    if suf in ("cpp", "cc", "cxx"):
        return "C++"
    if suf in ("h", "hpp"):
        return "Header"
    return suf


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    exts = {e.strip() for e in args.ext.split(",") if e.strip()}
    excludes = {p.strip() for p in args.exclude.split(",") if p.strip()}

    files = list(find_files(root, exts, excludes))

    per_file_counts: List[Tuple[Path, int]] = []
    ext_totals: defaultdict[str, int] = defaultdict(int)

    total = 0
    for p in files:
        c = count_file(p)
        per_file_counts.append((p, c))
        total += c
        ext_totals[group_by_ext(p)] += c

    print("LOC Summary (non-blank, non-comment lines)")
    print(f"Root: {root}")
    print(f"Files scanned: {len(files)}")
    print(f"Total LOC: {total}")
    print("")
    print("Breakdown by language:")
    for k in sorted(ext_totals.keys()):
        print(f"  {k:12s}: {ext_totals[k]}")

    if args.per_file:
        print("")
        print("Top files by LOC:")
        per_file_counts.sort(key=lambda t: t[1], reverse=True)
        for p, c in per_file_counts[: args.top]:
            print(f"  {c:6d} {p}")


if __name__ == "__main__":
    main()
