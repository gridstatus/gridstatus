# How to release

1. Bump version in `pyproject.toml`, `gridstatus/version.py`, and `gridstatus/tests/test_version.py`
2. Run `poetry run python ./docs/update_docs.py` to update methods in docs
  a. TODO: this fails with the error: "No such file or directory: 'availability.md'"
4. Run `make test-slow` to ensure slow test that CI doesn't check are passing
5. Update `CHANGELOG.md` to reflect changes made since the previous release and the date of the release
6. Make release on GitHub and tag it with a matching version number. The tag must start with `v` and be followed by the version number. For example, `v0.1.0`
  a. The `release.yaml` workflow will publish the package to PyPI after the release has been published.
1. Confirm package was uploaded to [PyPi](https://pypi.org/project/gridstatus/)
