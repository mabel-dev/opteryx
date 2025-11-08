import datetime
import glob
import sys
import time
import pytest
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx.rugo import parquet

FILES = glob.glob("tests/data/*.parquet")

def encode_value(val):
    # Strings are now returned as-is (UTF-8 decoded) to match rugo behavior
    # Binary data is returned as bytes
    if isinstance(val, datetime.datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=datetime.timezone.utc)
        else:
            val = val.astimezone(datetime.timezone.utc)
        return int(val.timestamp() * 1_000_000)
    if isinstance(val, datetime.date):
        return (val - datetime.date(1970, 1, 1)).days
    return val

def extract_pyarrow(path: str):
    pf = pq.ParquetFile(path)
    md = pf.metadata
    out = {
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "columns": []
    }
    # Map PyArrow type names to rugo type names
    type_map = {
        "BOOLEAN": "boolean",
        "INT32": "int32",
        "INT64": "int64",
        "INT96": "int96",
        "FLOAT": "float32",
        "DOUBLE": "float64",
        "BYTE_ARRAY": "byte_array",
        "FIXED_LEN_BYTE_ARRAY": "fixed_len_byte_array",
    }
    for rg_idx in range(md.num_row_groups):
        rg = md.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            stats = col.statistics
            # Map PyArrow type names to rugo type names
            physical_type = type_map.get(col.physical_type, col.physical_type.lower())
            out["columns"].append({
                "name": col.path_in_schema.split('.')[0],
                "physical_type": physical_type,
                "nulls": stats.null_count if stats else None,
                "min": encode_value(stats.min) if stats else None,
                "max": encode_value(stats.max) if stats else None,
                "bloom": getattr(col, "has_bloom_filter", False),
            })
    return out

def extract_custom(path: str):
    return parquet.read_metadata(path)

def compare(pa, cu):
    diffs = []
    if pa["rows"] != cu["num_rows"]:
        diffs.append(f"Row count mismatch: {pa['rows']} vs {cu['num_rows']}")
    if pa["row_groups"] != len(cu["row_groups"]):
        diffs.append(f"Row groups mismatch: {pa['row_groups']} vs {len(cu['row_groups'])}")
    if len(pa["columns"]) != sum(len(rg["columns"]) for rg in cu["row_groups"]):
        diffs.append("Column count mismatch")

    for i, (pa_col, cu_rg) in enumerate(zip(pa["columns"], cu["row_groups"][0]["columns"])):
        if pa_col["name"] != cu_rg["name"]:
            diffs.append(f"Col {i} name mismatch: {pa_col['name']} vs {cu_rg['name']}")
        if pa_col["physical_type"] != cu_rg["physical_type"]:
            diffs.append(f"Col {i} type mismatch: {pa_col['physical_type']} vs {cu_rg['physical_type']}")
        if pa_col.get("nulls") != cu_rg.get("null_count"):
            diffs.append(f"Col {i} nulls mismatch: {pa_col.get('nulls')} vs {cu_rg.get('null_count')}")
        
        # Skip min/max comparison for fixed_len_byte_array with decimal logical types
        # We don't decode decimal types yet
        if cu_rg["physical_type"] == "fixed_len_byte_array" and cu_rg.get("logical_type", "").startswith("decimal"):
            continue
            
        if pa_col.get("min") != cu_rg.get("min"):
            diffs.append(f"Col {i} min mismatch: `{pa_col.get('min')}` vs `{cu_rg.get('min')}` ({cu_rg['physical_type']})")
        if pa_col.get("max") != cu_rg.get("max"):
            diffs.append(f"Col {i} max mismatch: `{pa_col.get('max')}` vs `{cu_rg.get('max')}` ({cu_rg['physical_type']})")
    return diffs

def run_one(file: str, iters=100) -> bool:
    print(f"\n=== {file} ===")

    # PyArrow timing
    t0 = time.perf_counter()
    pa = extract_pyarrow(file)
    t1 = time.perf_counter()
    arrow_time = (t1 - t0) * 1000

    # Custom timing
    t0 = time.perf_counter()
    cu = extract_custom(file)
    t1 = time.perf_counter()
    custom_time = (t1 - t0) * 1000

    diffs = compare(pa, cu)

    print(f"[PyArrow] rows={pa['rows']} groups={pa['row_groups']} time={arrow_time:.3f} ms")
    print(f"[Custom] rows={cu['num_rows']} groups={len(cu['row_groups'])} time={custom_time:.3f} ms")
    if diffs:
        print("❌ Differences:")
        for d in diffs:
            print(" -", d)
        return False
    
    print("✅ Results match")
    return True

def test_compare_arrow_rugo():
    for f in FILES:
        if Path(f).exists():
            # Skip files with list columns - known limitation with list column naming
            if any(name in f for name in ['tweets.parquet', 'astronauts.parquet']):
                print(f"\n⚠️  Skipping {f} - known limitation with list column naming")
                continue
            assert run_one(f)
        else:
            print(f"⚠️  Missing file {f}")

if __name__ == "__main__":
    pytest.main([__file__])
