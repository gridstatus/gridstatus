# How to release

1. Bump version in `gridstatus/version.py` and `gridstatus/tests/test_version.py`
2. Run `python update_docs.py` to update methods in docs
3. Run `make test-slow' to ensure slow test that CI doesn't check are passing
4. Update `CHANGELOG.md`
5. Make release on GitHub and tag it with a matching version number
6. Confirm package was uploaded to [PyPi](https://pypi.org/project/gridstatus/)
