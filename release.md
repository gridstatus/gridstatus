# How to release

1. Bump version in `pyproject.toml`, `gridstatus/version.py` and `gridstatus/tests/test_version.py`
2. Run `poetry run python ./docs/update_docs.py` to update methods in docs
3. Run `make test-slow' to ensure slow test that CI doesn't check are passing
4. Update `CHANGELOG.md` to reflect changes made since the previous release and the date of the release
5. Make release on GitHub and tag it with a matching version number
6. Confirm package was uploaded to [PyPi](https://pypi.org/project/gridstatus/)
