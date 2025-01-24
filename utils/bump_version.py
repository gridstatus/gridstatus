import argparse
import datetime
import re
import sys
from pathlib import Path


def validate_version(version: str) -> bool:
    """Validate version string matches format like 0.30.0"""
    pattern = r"^\d+\.\d+\.\d+$"
    return bool(re.match(pattern, version))


def update_version_py(file_path: Path, old_version: str, new_version: str) -> bool:
    """Update version in version.py file"""
    content = file_path.read_text()
    updated = content.replace(
        f'__version__ = "{old_version}"',
        f'__version__ = "{new_version}"',
    )
    if updated != content:
        file_path.write_text(updated)
        return True
    return False


def update_test_version_py(file_path: Path, old_version: str, new_version: str) -> bool:
    """Update version in test_version.py file"""
    content = file_path.read_text()
    updated = content.replace(
        f'assert __version__ == "{old_version}"',
        f'assert __version__ == "{new_version}"',
    )
    if updated != content:
        file_path.write_text(updated)
        return True
    return False


def update_pyproject_toml(file_path: Path, old_version: str, new_version: str) -> bool:
    """Update version in pyproject.toml file"""
    content = file_path.read_text()
    updated = content.replace(
        f'version = "{old_version}"',
        f'version = "{new_version}"',
    )
    if updated != content:
        file_path.write_text(updated)
        return True
    return False


def update_citation_cff(file_path: Path, old_version: str, new_version: str) -> bool:
    """Update version in CITATION.cff file"""
    content = file_path.read_text()
    updated = content.replace(f"version: {old_version}", f"version: {new_version}")
    # Also update the date-released with the current date. Use regex to find the line

    updated = updated.replace(
        re.search(r"date-released: \d{4}-\d{2}-\d{2}", updated).group(0),
        f"date-released: {datetime.datetime.now().strftime('%Y-%m-%d')}",
    )

    if updated != content:
        file_path.write_text(updated)
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update version numbers across project files",
    )
    parser.add_argument("old_version", help="Current version number (e.g., 0.30.0)")
    parser.add_argument("new_version", help="New version number (e.g., 0.31.0)")
    args = parser.parse_args()

    # Validate version numbers
    if not all(validate_version(v) for v in [args.old_version, args.new_version]):
        print("Error: Version numbers must be in format X.Y.Z (e.g., 0.30.0)")
        sys.exit(1)

    # Define files to update
    files = {
        "gridstatus/version.py": update_version_py,
        "gridstatus/tests/test_version.py": update_test_version_py,
        "pyproject.toml": update_pyproject_toml,
        "CITATION.cff": update_citation_cff,
    }

    # Track success
    success_count = 0
    for file_path, update_func in files.items():
        path = Path(file_path)
        if not path.exists():
            print(f"Warning: {file_path} not found")
            continue

        try:
            updated = update_func(path, args.old_version, args.new_version)
            if updated:
                print(f"Updated {file_path}")
                success_count += 1
            else:
                print(f"No changes needed in {file_path}")
        except Exception as e:
            print(f"Error updating {file_path}: {e}")

    print(
        f"\nUpdated {success_count} files from version {args.old_version} to {args.new_version}",
    )


if __name__ == "__main__":
    main()
