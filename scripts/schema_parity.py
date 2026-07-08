"""Schema-parity harness for the pandas -> polars migration.

Snapshots the output schema (columns, dtypes, row count) of ISO ``get_*``
methods called with fixed dates, and diffs snapshots across branches.

Intended workflow:

1. Baseline (pre-migration branch, own venv via git worktree)::

       git worktree add ../gridstatus-baseline main
       cd ../gridstatus-baseline && uv sync
       uv run python <this repo>/scripts/schema_parity.py snapshot \
           --iso ISONE --output /tmp/isone_baseline.json

2. Converted (working tree)::

       uv run python scripts/schema_parity.py snapshot \
           --iso ISONE --output gridstatus/tests/schema_snapshots/isone.json

3. Diff::

       uv run python scripts/schema_parity.py diff \
           /tmp/isone_baseline.json gridstatus/tests/schema_snapshots/isone.json

Columns and normalized dtypes must match exactly; row-count drift is reported
but not fatal (real-time feeds move between runs). The script is standalone so
the identical file can be executed in the baseline worktree.
"""

import argparse
import json
import re
import sys
import traceback
from typing import Any

FIXED_DATE = "2026-06-15"
FIXED_END = "2026-06-17"
DST_DATE = "2026-03-08"

SKIP_METHODS = {
    "get_raw_interconnection_queue",
    "get_status",
}

# Methods that need extra kwargs beyond date/end. Keyed by (iso, method);
# iso=None applies to any ISO exposing the method.
KWARG_OVERRIDES: dict[tuple[str | None, str], dict[str, Any]] = {
    (None, "get_lmp"): {"market": "DAY_AHEAD_HOURLY"},
    ("CAISO", "get_lmp"): {
        "market": "DAY_AHEAD_HOURLY",
        "locations": ["TH_NP15_GEN-APND"],
    },
    ("Ercot", "get_lmp"): {"market": "REAL_TIME_SCED"},
    ("SPP", "get_lmp"): {"market": "REAL_TIME_5_MIN", "location_type": "Hub"},
    (None, "get_spp"): {"market": "DAY_AHEAD_HOURLY"},
}


def _normalize_dtype(dtype: object) -> str:
    """Map pandas and polars dtypes onto a shared vocabulary for diffing."""
    s = str(dtype)

    m = re.match(r"datetime64\[\w+(?:, (?P<tz>.+))?\]", s)
    if m:
        return f"datetime[{m.group('tz') or 'naive'}]"

    m = re.match(r"Datetime\(time_unit='\w+', time_zone=(?P<tz>.+)\)", s)
    if m:
        tz = m.group("tz").strip("'\"")
        return f"datetime[{'naive' if tz == 'None' else tz}]"
    if s == "Datetime":
        return "datetime[naive]"

    mapping = {
        "object": "string",
        "string": "string",
        "str": "string",
        "String": "string",
        "Utf8": "string",
        "category": "string",
        "Categorical": "string",
        "bool": "bool",
        "boolean": "bool",
        "Boolean": "bool",
        "date": "date",
        "Date": "date",
    }
    if s in mapping:
        return mapping[s]

    if re.match(r"(Int|UInt|int|uint)\d+", s):
        return "int"
    if re.match(r"(Float|float)\d+", s):
        return "float"
    if s == "Null":
        return "null"

    return s


def _frame_schema(df: object) -> dict[str, Any] | None:
    """Return the schema dict for a pandas or polars DataFrame, else None."""
    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            return {
                "columns": df.columns,
                "dtypes": {c: _normalize_dtype(t) for c, t in df.schema.items()},
                "row_count": df.height,
                "frame_type": "polars",
            }
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return {
                "columns": list(df.columns),
                "dtypes": {
                    c: _normalize_dtype(t) for c, t in df.dtypes.to_dict().items()
                },
                "row_count": len(df),
                "frame_type": "pandas",
            }
    except ImportError:
        pass

    return None


def _snapshot_result(result: object) -> dict[str, Any]:
    schema = _frame_schema(result)
    if schema is not None:
        return schema
    if isinstance(result, dict):
        return {
            "frame_type": "dict",
            "keys": {
                str(k): _frame_schema(v) or {"type": type(v).__name__}
                for k, v in result.items()
            },
        }
    return {"frame_type": type(result).__name__}


def _call_method(iso: object, method_name: str) -> dict[str, Any]:
    import inspect

    method = getattr(iso, method_name)
    sig = inspect.signature(method)
    params = sig.parameters

    kwargs: dict[str, Any] = {}
    override = KWARG_OVERRIDES.get(
        (type(iso).__name__, method_name),
    ) or KWARG_OVERRIDES.get((None, method_name))
    if override:
        kwargs.update(override)

    if "date" in params:
        kwargs["date"] = FIXED_DATE

    required = [
        p
        for name, p in params.items()
        if p.default is inspect.Parameter.empty
        and p.kind
        in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
        and name not in kwargs
        and name != "self"
    ]
    if required:
        return {
            "skipped": f"requires args: {[p.name for p in required]}",
        }

    try:
        result = method(**kwargs)
    except NotImplementedError:
        return {"skipped": "NotImplementedError"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

    return _snapshot_result(result)


def snapshot(iso_name: str, output: str, methods: list[str] | None) -> None:
    import gridstatus

    iso_cls = getattr(gridstatus, iso_name)
    iso = iso_cls()

    method_names = sorted(
        name
        for name in dir(iso)
        if name.startswith("get_")
        and callable(getattr(iso, name))
        and name not in SKIP_METHODS
    )
    if methods:
        method_names = [m for m in method_names if m in methods]

    snap: dict[str, Any] = {"iso": iso_name, "fixed_date": FIXED_DATE, "methods": {}}
    for name in method_names:
        print(f"  {iso_name}.{name} ...", flush=True)
        try:
            snap["methods"][name] = _call_method(iso, name)
        except Exception:
            snap["methods"][name] = {"error": traceback.format_exc(limit=3)}

    with open(output, "w") as f:
        json.dump(snap, f, indent=2, default=str)
    print(f"wrote {output}")


def diff(baseline_path: str, converted_path: str) -> int:
    with open(baseline_path) as f:
        baseline = json.load(f)
    with open(converted_path) as f:
        converted = json.load(f)

    failures = 0
    warnings = 0
    base_methods = baseline["methods"]
    conv_methods = converted["methods"]

    for name in sorted(set(base_methods) | set(conv_methods)):
        b = base_methods.get(name)
        c = conv_methods.get(name)
        if b is None or c is None:
            print(f"[WARN] {name}: only present in one snapshot")
            warnings += 1
            continue
        if "skipped" in b or "skipped" in c:
            continue
        if "error" in b or "error" in c:
            if ("error" in b) != ("error" in c):
                print(
                    f"[FAIL] {name}: error mismatch "
                    f"(baseline: {b.get('error', 'ok')!r}, "
                    f"converted: {c.get('error', 'ok')!r})",
                )
                failures += 1
            continue

        if "columns" not in b or "columns" not in c:
            continue

        if b["columns"] != c["columns"]:
            print(f"[FAIL] {name}: columns differ")
            print(f"       baseline:  {b['columns']}")
            print(f"       converted: {c['columns']}")
            failures += 1
            continue

        dtype_mismatches = {
            col: (b["dtypes"][col], c["dtypes"][col])
            for col in b["columns"]
            if b["dtypes"][col] != c["dtypes"][col]
        }
        if dtype_mismatches:
            print(f"[FAIL] {name}: dtypes differ: {dtype_mismatches}")
            failures += 1
            continue

        if b["row_count"] != c["row_count"]:
            print(
                f"[WARN] {name}: row count drift "
                f"({b['row_count']} -> {c['row_count']})",
            )
            warnings += 1

        print(f"[OK]   {name}")

    print(f"\n{failures} failures, {warnings} warnings")
    return 1 if failures else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    snap_p = sub.add_parser("snapshot", help="snapshot get_* schemas for an ISO")
    snap_p.add_argument("--iso", required=True, help="ISO class name, e.g. ISONE")
    snap_p.add_argument("--output", required=True)
    snap_p.add_argument(
        "--methods",
        nargs="*",
        help="only snapshot these method names",
    )

    diff_p = sub.add_parser("diff", help="diff two snapshot files")
    diff_p.add_argument("baseline")
    diff_p.add_argument("converted")

    args = parser.parse_args()
    if args.command == "snapshot":
        snapshot(args.iso, args.output, args.methods)
    else:
        sys.exit(diff(args.baseline, args.converted))


if __name__ == "__main__":
    main()
