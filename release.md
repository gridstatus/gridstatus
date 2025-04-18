# How to release

1. Run `uv run utils/bump_version.py <old_version> <new_version>` to bump versions in `pyproject.toml`, `gridstatus/version.py`, `gridstatus/tests/test_version.py`, and `CITATION.cff`
2. Run `uv run ./docs/update_docs.py` to update methods in docs
3. Run `make test-slow` to ensure slow test that CI doesn't check are passing
4. Update `CHANGELOG.md` to reflect changes made since the previous release and the date of the release
5. Create a PR with changes. After merging:
6. Make release on GitHub and tag it with a matching version number. The tag must start with `v` and be followed by the version number. For example, `v0.1.0`
  a. The `release.yaml` workflow will publish the package to PyPI after the release has been published.
1. Confirm package was uploaded to [PyPi](https://pypi.org/project/gridstatus/)
