# How to release

1. Bump version in `gridstatus/version.py` and `gridstatus/tests/test_version.py`
2. Run `make test-slow' to ensure slow test that CI doesn't check are passing
3. Update `CHANGELOG.md`
4. Make release on GitHub and tag it with a matching version number
5. Confirm package was uploaded to [PyPi](https://pypi.org/project/gridstatus/)
