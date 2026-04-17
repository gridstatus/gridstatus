#!/usr/bin/env python3
"""Sync VCR test fixtures between local filesystem and S3.

Usage:
    python scripts/fixtures.py download [--iso ISO_NAME]
    python scripts/fixtures.py upload [--iso ISO_NAME] [--force]
    python scripts/fixtures.py cache-paths [--iso ISO_NAME]
    python scripts/fixtures.py manifest [--iso ISO_NAME]

Commands:
    download     Download fixtures from S3 (public, no credentials needed).
                 Exits non-zero if any per-source sync fails.
    upload       Upload fixtures to S3 (requires AWS credentials). Refuses
                 by default if any cassette contains a 4xx/5xx response;
                 pass --force to override.
    cache-paths  Output local fixture directory paths (for GHA cache action)
    manifest     Generate SHA256 hash of S3 fixture contents (for cache key)

Options:
    --iso ISO    Only operate on a specific ISO (e.g., caiso, ercot, pjm)
                 If omitted, operates on all ISOs.
    --force      upload only: skip the 4xx/5xx cassette safety scan.
"""

import argparse
import hashlib
import os
import subprocess
import sys

S3_BUCKET = "gridstatus-test-fixtures"
S3_PREFIX = "vcr_cassettes"
FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "gridstatus",
    "tests",
    "fixtures",
)

# Single source of truth: maps CI matrix ISO names to fixture source directories.
# Each ISO may have multiple fixture directories (e.g., ercot has ercot/ and ercot_api/).
ISO_FIXTURE_MAP: dict[str, list[str]] = {
    "aeso": ["aeso"],
    "caiso": ["caiso", "caiso_save_to"],
    "ercot": ["ercot", "ercot_api"],
    "isone": ["isone", "isone_api"],
    "miso": ["miso", "miso_api"],
    "nyiso": ["nyiso"],
    "pjm": ["pjm"],
    "spp": ["spp"],
    "ieso": ["ieso"],
    "eia": ["eia"],
    "misc": ["gridstatus"],
}


def get_fixture_dirs(iso: str | None) -> list[str]:
    """Return the list of fixture source directory names for an ISO or all ISOs."""
    if iso is None:
        # All unique source directories across all ISOs
        seen: set[str] = set()
        result: list[str] = []
        for dirs in ISO_FIXTURE_MAP.values():
            for d in dirs:
                if d not in seen:
                    seen.add(d)
                    result.append(d)
        return result

    if iso not in ISO_FIXTURE_MAP:
        print(f"Error: Unknown ISO '{iso}'.", file=sys.stderr)
        print(f"Valid ISOs: {', '.join(sorted(ISO_FIXTURE_MAP))}", file=sys.stderr)
        sys.exit(1)
    return ISO_FIXTURE_MAP[iso]


def s3_path(source: str) -> str:
    return f"s3://{S3_BUCKET}/{S3_PREFIX}/{source}/"


def local_path(source: str) -> str:
    return os.path.normpath(
        os.path.join(FIXTURES_DIR, source, "vcr_cassettes"),
    )


def run_aws(
    args: list[str],
    check: bool = True,
    quiet: bool = False,
) -> subprocess.CompletedProcess:
    cmd = ["aws"] + args
    if not quiet:
        print(f"  $ {' '.join(cmd)}", file=sys.stderr)
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def cmd_download(iso: str | None) -> None:
    """Download fixtures from S3 (public bucket, no credentials needed).

    Exits non-zero if any per-source sync fails so CI can distinguish a
    partial outage from a clean download.
    """
    sources = get_fixture_dirs(iso)
    print(f"Downloading fixtures for: {', '.join(sources)}")

    failed: list[str] = []
    for source in sources:
        local = local_path(source)
        os.makedirs(local, exist_ok=True)
        s3 = s3_path(source)
        print(f"\n[{source}] {s3} -> {local}")
        result = run_aws(
            ["s3", "sync", s3, local, "--no-sign-request"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  Error: {result.stderr.strip()}", file=sys.stderr)
            failed.append(source)
        else:
            print("  Done.")

    if failed:
        print(
            f"\nDownload failed for {len(failed)} source(s): {', '.join(failed)}",
            file=sys.stderr,
        )
        sys.exit(1)


# Cassette filename substrings that signal the test intentionally exercises an
# error path (e.g. asserts NoDataFoundException). These are allowed to contain
# 4xx/5xx responses — the error response IS the recording the test needs.
EXPECTED_ERROR_PATTERNS: tuple[str, ...] = (
    "_no_data",
    "_raises_error",
    "_too_far_in_past",
    "_too_far_in_future",
    "_in_past_raises",
    "_invalid_",
    "_not_supported",
)


def _is_expected_error_cassette(name: str) -> bool:
    return any(pat in name for pat in EXPECTED_ERROR_PATTERNS)


def _scan_cassettes_for_errors(local: str) -> list[tuple[str, str]]:
    """Scan all .yaml cassettes under `local` for HTTP 4xx/5xx responses.

    Returns a list of (cassette_path, first_matching_line). A non-empty
    return indicates cassettes that should not be uploaded. Cassettes whose
    filename matches EXPECTED_ERROR_PATTERNS are skipped — those tests
    intentionally record error responses to verify the library's error path.
    """
    problems: list[tuple[str, str]] = []
    for root, _dirs, files in os.walk(local):
        for name in files:
            if not name.endswith(".yaml"):
                continue
            if _is_expected_error_cassette(name):
                continue
            path = os.path.join(root, name)
            try:
                with open(path, encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        stripped = line.strip()
                        # VCR serialises status as "code: NNN" — flag 4xx/5xx.
                        if stripped.startswith("code: 4") or stripped.startswith(
                            "code: 5",
                        ):
                            problems.append((path, stripped))
                            break
            except OSError as e:
                print(f"  Warning: could not read {path}: {e}", file=sys.stderr)
    return problems


def cmd_upload(iso: str | None, force: bool = False) -> None:
    """Upload fixtures to S3 (requires AWS credentials).

    Refuses to upload cassettes containing 4xx/5xx responses unless
    ``--force`` is passed, since those will poison CI for every ISO.
    """
    sources = get_fixture_dirs(iso)
    print(f"Uploading fixtures for: {', '.join(sources)}")

    if not force:
        all_problems: list[tuple[str, str]] = []
        for source in sources:
            local = local_path(source)
            if os.path.isdir(local):
                all_problems.extend(_scan_cassettes_for_errors(local))
        if all_problems:
            print(
                f"\nRefusing to upload — {len(all_problems)} cassette(s) "
                "contain 4xx/5xx responses:",
                file=sys.stderr,
            )
            for path, line in all_problems[:20]:
                print(f"  {path}: {line}", file=sys.stderr)
            if len(all_problems) > 20:
                print(f"  ... and {len(all_problems) - 20} more", file=sys.stderr)
            print(
                "\nDelete the bad cassettes and re-record, or pass --force to "
                "upload anyway.",
                file=sys.stderr,
            )
            sys.exit(1)

    for source in sources:
        local = local_path(source)
        if not os.path.isdir(local):
            print(f"\n[{source}] Skipping — {local} does not exist")
            continue
        s3 = s3_path(source)
        print(f"\n[{source}] {local} -> {s3}")
        result = run_aws(
            ["s3", "sync", local, s3, "--delete", "--exclude", ".DS_Store"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  Error: {result.stderr.strip()}", file=sys.stderr)
            sys.exit(1)
        else:
            print("  Done.")


def cmd_cache_paths(iso: str | None) -> None:
    """Output fixture directory paths relative to repo root (for GHA cache action)."""
    sources = get_fixture_dirs(iso)
    for source in sources:
        # Output relative path from repo root for actions/cache compatibility
        print(f"gridstatus/tests/fixtures/{source}/vcr_cassettes")


def cmd_manifest(iso: str | None) -> None:
    """Generate a SHA256 hash of S3 fixture listing for cache key busting.

    Uses `aws s3 ls --recursive --no-sign-request` which returns file sizes
    and paths (metadata only, no file downloads). If any cassette is
    added/changed/removed, the hash changes.
    """
    sources = get_fixture_dirs(iso)
    hasher = hashlib.sha256()

    for source in sources:
        s3 = s3_path(source)
        result = run_aws(
            ["s3", "ls", s3, "--recursive", "--no-sign-request"],
            check=False,
            quiet=True,
        )
        if result.returncode != 0:
            # If the prefix doesn't exist yet, that's fine — hash empty string
            hasher.update(f"{source}:empty\n".encode())
        else:
            # Sort lines for deterministic hashing regardless of S3 listing order
            lines = sorted(result.stdout.strip().splitlines())
            for line in lines:
                hasher.update(f"{source}:{line}\n".encode())

    print(hasher.hexdigest())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync VCR test fixtures between local filesystem and S3.",
    )
    parser.add_argument(
        "command",
        choices=["download", "upload", "cache-paths", "manifest"],
        help="Command to run",
    )
    parser.add_argument(
        "--iso",
        default=None,
        help="Operate on a specific ISO only (e.g., caiso, ercot, pjm)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="upload: skip the 4xx/5xx cassette safety scan",
    )
    args = parser.parse_args()

    if args.command == "upload":
        cmd_upload(args.iso, force=args.force)
    elif args.command == "download":
        cmd_download(args.iso)
    elif args.command == "cache-paths":
        cmd_cache_paths(args.iso)
    elif args.command == "manifest":
        cmd_manifest(args.iso)


if __name__ == "__main__":
    main()
