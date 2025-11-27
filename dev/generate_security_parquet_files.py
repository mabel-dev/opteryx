#!/usr/bin/env python3
"""
One-off script to generate fake security finding Parquet files.

Creates a number of Parquet files (default 100) each containing a number of
fake security finding records (default 100). The fields are:

- id (monotonically increasing integer)
- server (string)
- patch_id (string, like MS KB item)
- cves (list of CVE strings)
- first_found (datetime)
- times_found (integer)
- risk_score (double between 0.0 and 10.0)

Usage:
    python dev/generate_security_parquet_files.py --count 100 --per-file 100 --out ./dev/security_parquets

This script uses pandas and pyarrow. Install:
    pip install pandas pyarrow numpy

"""

from __future__ import annotations

import argparse
import random
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def random_server(idx: int) -> str:
    # Basename and a shard number so it's easy to find duplicates
    names = [
        "alpha", "beta", "gamma", "delta", "omega", "lambda", "zeta", "theta",
        "sigma", "kappa",
    ]
    return f"{random.choice(names)}-srv-{idx % 100}"


def random_patch_id() -> str:
    # Windows KB-style string: KB followed by 5-7 digits
    return f"KB{random.randint(10000, 9999999)}"


def random_cves() -> list[str]:
    count = random.randint(1, 3)
    cves = []
    for _ in range(count):
        year = random.randint(2017, datetime.now().year)
        # NNNN to NNNNNN
        number = random.randint(1000, 999999)
        cves.append(f"CVE-{year}-{number}")
    return cves


def random_first_found() -> datetime:
    # Random datetime in the last 3 years
    now = datetime.now(timezone.utc)
    days = random.randint(0, 3 * 365)
    seconds = random.randint(0, 24 * 60 * 60 - 1)
    return now - timedelta(days=days, seconds=seconds)


def random_times_found() -> int:
    # Use Poisson-like distribution to bias to small numbers but allow larger ones
    lam = 2.5
    # numpy.poisson returns ints >= 0
    n = np.random.poisson(lam)
    return int(n)


def random_risk_score() -> float:
    # Use a skewed distribution so most are lower risk but some are high
    # uniform then square root to bias towards lower numbers
    r = random.random()
    return round(10.0 * (r ** 0.7), 3)  # 0..10


def gen_record(rec_id: int) -> dict:
    ts = random_first_found()
    return {
        "id": rec_id,
        "server": random_server(rec_id),
        "patch_id": random_patch_id(),
        "cves": random_cves(),
        "first_found": ts,
        "times_found": random_times_found(),
        "risk_score": random_risk_score(),
    }


def make_file(file_path: Path, start_id: int, n_records: int, seed: int | None = None):
    if seed is not None:
        random.seed(seed + start_id)
        np.random.seed(seed + start_id)

    ids = []
    servers = []
    patch_ids = []
    cves_list = []
    first_found_list = []
    times_found_list = []
    risk_scores = []

    rec_id = start_id
    for i in range(n_records):
        r = gen_record(rec_id)
        ids.append(r["id"])
        servers.append(r["server"])
        patch_ids.append(r["patch_id"])
        cves_list.append(r["cves"])
        first_found_list.append(r["first_found"])
        times_found_list.append(r["times_found"])
        risk_scores.append(r["risk_score"])
        rec_id += 1

    # Build pyarrow arrays with types
    arr_id = pa.array(ids, type=pa.int64())
    arr_server = pa.array(servers, type=pa.string())
    arr_patch = pa.array(patch_ids, type=pa.string())
    arr_cves = pa.array(cves_list, type=pa.list_(pa.string()))
    arr_first = pa.array(first_found_list, type=pa.timestamp("ms", tz="UTC"))
    arr_times = pa.array(times_found_list, type=pa.int64())
    arr_risk = pa.array(risk_scores, type=pa.float64())

    # Build table ensuring specific schema
    table = pa.Table.from_arrays(
        [arr_id, arr_server, arr_patch, arr_cves, arr_first, arr_times, arr_risk],
        names=["id", "server", "patch_id", "cves", "first_found", "times_found", "risk_score"],
    )

    # Write parquet
    pq.write_table(table, file_path, use_deprecated_int96_timestamps=False)
    return rec_id - start_id  # number of records written


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet files with fake security findings")
    parser.add_argument("--count", type=int, default=100, help="Number of files to create (default 100)")
    parser.add_argument("--per-file", type=int, default=100, help="Records per file (default 100)")
    parser.add_argument("--out", type=str, default="dev/security_parquets", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--prefix", type=str, default="security_findings", help="Filename prefix")

    args = parser.parse_args()

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    file_count = args.count
    per_file = args.per_file

    seed = args.seed

    next_id = 1
    print(f"Generating {file_count} parquet files with {per_file} records each: {file_count * per_file} records total")
    print("Writing to:", outdir)

    for i in range(file_count):
        fname = f"{args.prefix}-{i:04d}.parquet"
        path = outdir / fname
        # Pass a slight offset to seed so each file is different but reproducible
        wrote = make_file(path, start_id=next_id, n_records=per_file, seed=seed)
        next_id += wrote
        if (i + 1) % 10 == 0 or i == file_count - 1:
            print(f"  - {i+1}/{file_count} written (last id {next_id - 1})")

    print("Done.")


if __name__ == "__main__":
    main()
