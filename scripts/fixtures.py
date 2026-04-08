#!/usr/bin/env python3
"""Sync VCR test fixtures between local filesystem and S3.

Usage:
    python scripts/fixtures.py download [--iso ISO_NAME]
    python scripts/fixtures.py upload [--iso ISO_NAME]
    python scripts/fixtures.py cache-paths [--iso ISO_NAME]
    python scripts/fixtures.py manifest [--iso ISO_NAME]

Commands:
    download     Download fixtures from S3 (public, no credentials needed)
    upload       Upload fixtures to S3 (requires AWS credentials)
    cache-paths  Output local fixture directory paths (for GHA cache action)
    manifest     Generate SHA256 hash of S3 fixture contents (for cache key)

Options:
    --iso ISO    Only operate on a specific ISO (e.g., caiso, ercot, pjm)
                 If omitted, operates on all ISOs.
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
    """Download fixtures from S3 (public bucket, no credentials needed)."""
    sources = get_fixture_dirs(iso)
    print(f"Downloading fixtures for: {', '.join(sources)}")

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
            print(f"  Warning: {result.stderr.strip()}", file=sys.stderr)
        else:
            print("  Done.")


def cmd_upload(iso: str | None) -> None:
    """Upload fixtures to S3 (requires AWS credentials)."""
    sources = get_fixture_dirs(iso)
    print(f"Uploading fixtures for: {', '.join(sources)}")

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
    args = parser.parse_args()

    commands = {
        "download": cmd_download,
        "upload": cmd_upload,
        "cache-paths": cmd_cache_paths,
        "manifest": cmd_manifest,
    }
    commands[args.command](args.iso)


if __name__ == "__main__":
    main()
