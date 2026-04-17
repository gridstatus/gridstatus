# Contributing

Thank you for considering contributing to our project! We are grateful for any time and effort you put into making this project better.

There are several ways you can contribute:

* Submitting bug reports and feature requests

* Submitting pull requests for code changes

* Adding documentation or examples

* Participating in discussions or providing feedback

Before contributing, please read this guide. If you have any questions, feel free to open a GitHub issue.

## Submitting Bug Reports and Feature Requests

If you find a bug in the code or have an idea for a new feature, please [search for an existing issue](https://github.com/gridstatus/gridstatus/issues) or [open a new issue](https://github.com/gridstatus/gridstatus/issues/new) and provide as much detail as possible.

## Submitting Pull Requests

We welcome pull requests for code changes! Before starting work on a pull request, please check with a maintainer to ensure that no one else is already working on the same change. Typically, this is done by posting a comment on the appropriate GitHub issue stating that you want to work on it.

Before working on a PR, you should first create a fork of `gridstatus` in your GitHub account and do your work in a new branch that you create in the fork. Read more [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests) about collaborating with Pull Requests.

When submitting a pull request, please make sure to:

* Write tests to cover any changes you make

* Add any or update docstrings relevant to your change

* Follow the existing code styles in place

* Include a description of the changes you made and provide any additional information that will be helpful to the reviewer of your code

* If relevant to users of the library, add details of your change to `CHANGELOG.md`.


## Setting up a Development Environment


To set up a development environment for this project, you will need to:

* Install Python 3.11 or higher

* Fork the repository and clone it to your local machine. For a PR, create a new branch in your fork.

* Install the project dependencies by running:

```shell
# Configure uv to use a virtual environment
uv venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install the project dependencies
uv sync
```

* Installing the dev dependencies enables a pre-commit hook that ensures linting has been run before committing

The best way to ensure everything is installed correctly is by running the tests. They should all pass.

## Environment Variables

* Copy `.env.template` into a new file called `.env` and fill in the appropriate values for the environment variables. `.env` should not be committed to the repository.
* Fill out these variables if you want to use the EIA or ERCOT APIs. To make sure your .env is accessible to the uv runtime environment, you can set the `UV_ENV_FILE` environment variable to the path of your .env file. `export UV_ENV_FILE=path/to/.env`


## Running Tests and Linting

To ensure that your changes are correct and follow our style guide, we ask you to run the tests and linting before submitting a pull request. You can use the following commands to do so:

```
# Run all tests
make test

# Run slow tests marked with @pytest.mark.slow
make test-slow

# Lint the code
make lint

# Fix linting errors
make lint-fix
```

We use `pytest` for testing, so you can also run the test directly with the `pytest` command.


## Adding Documentation and Examples

To add documentation and examples to the project, follow these steps:

1. Create a new `.ipynb` file with cell outputs saved to the appropriate folder in the docs/Examples directory. Make sure to save the outputs as you want them to be displayed, as the documentation build process will not run the notebook.

2. Add a link to the notebook in `docs/Examples/<folder>/index.md` to include it in the table of contents.

3. Build the documentation to confirm that the notebook is visible. You can do this locally by running `make docs` from the root directory. Read the Docs will also build the documentation on every pull request, which you can view by clicking on the details of the Read The Doc GitHub action from the PR.

We welcome contributions to the documentation and examples, and appreciate any efforts to improve them.

## Code Review Process

All pull requests will be reviewed by one of our maintainers before being merged. We will review pull requests for:

* Correctness

* Code style

* Test coverage

We may ask for additional changes or clarification before merging a pull request.

## VCR Cassettes (Test Fixtures)

Tests that make HTTP requests to ISO APIs use [VCR.py](https://vcrpy.readthedocs.io/) to record and replay responses. Recorded responses ("cassettes") are stored on S3 and cached in GitHub Actions so CI tests run without live API access.

### How It Works

- **In CI** (`VCR_RECORD_MODE=none`): tests play back cassettes from S3. If a cassette is missing, the test is skipped automatically.
- **Locally** (`VCR_RECORD_MODE=new_episodes`): missing cassettes are recorded from live API calls. Existing cassettes are replayed.
- Tests marked `@pytest.mark.integration` are excluded from CI test-iso jobs (they require live API access or use relative dates like "today"/"latest").

### Adding a New Test with a VCR Cassette

1. **Write your test** using the VCR wrapper for your ISO:

```python
# At the top of the test file, the VCR instance is already set up:
# api_vcr = setup_vcr(source="caiso", record_mode=RECORD_MODE)

def test_get_my_new_dataset(self):
    date = pd.Timestamp("2026-03-15")  # Use a fixed, recent date
    with api_vcr.use_cassette("test_get_my_new_dataset_2026-03-15.yaml"):
        df = self.iso.get_my_new_dataset(date)
    assert df.shape[0] > 0
```

2. **Record the cassette locally** — run the test with your API credentials in `.env`:

```bash
uv run pytest -s -vv -k "test_get_my_new_dataset" gridstatus/tests/source_specific/test_<iso>.py
```

3. **Verify the cassette is clean** — `make fixtures-upload` scans every cassette for `code: 4xx`/`code: 5xx` responses and refuses to upload if any are found. You can also scan manually:

```bash
grep "code: [45]" gridstatus/tests/fixtures/<iso>/vcr_cassettes/test_get_my_new_dataset_2026-03-15.yaml
# Should return nothing
```

4. **Upload to S3** — requires AWS credentials:

```bash
make fixtures-upload iso=<iso>
# Override the 4xx/5xx safety scan if you really need to:
make fixtures-upload iso=<iso> force=1
```

5. **Commit and push** — the cassette files are NOT committed to git. CI downloads them from S3 (with GHA caching).

### Important Guidelines

- **Use fixed dates** — never use `"today"`, `"latest"`, or `pd.Timestamp.now()` in VCR-wrapped tests. These generate different URLs each run, breaking cassette matching.
- **Use recent dates** — ISO APIs typically expire data after 30-90 days. Use dates within the last few weeks.
- **Don't upload error cassettes** — if the API returned 4xx/5xx errors, delete the cassette and re-record.
- **Mark live-only tests as integration** — if a test inherently needs live data (e.g., uses relative dates, or the API uses cache-busting params), add `@pytest.mark.integration`.
- **Cassette naming** — use descriptive names including the date: `test_get_lmp_historical_2026-03-15.yaml`.

### Makefile Targets

```bash
make fixtures-download               # Download all fixtures from S3
make fixtures-download-iso iso=caiso  # Download fixtures for one ISO
make fixtures-upload                  # Upload all fixtures to S3
make fixtures-upload iso=caiso        # Upload fixtures for one ISO
```

### Key Files

- `gridstatus/tests/vcr_utils.py` — VCR configuration, record mode logic, custom matchers
- `gridstatus/tests/fixtures/<iso>/vcr_cassettes/` — local cassette storage (gitignored)
- `scripts/fixtures.py` — S3 sync script (`ISO_FIXTURE_MAP` is the source of truth for ISO→fixture directory mapping)
- `.github/workflows/tests-pr.yaml` — CI workflow that downloads fixtures from S3 with GHA caching

## CI/CD

When you submit a PR, the following actions are automatically performed

1. GitHub Actions runs the tests in all supported Python versions
2. GitHub Actions ensures the code is properly linted
3. Our documentation host, Read the Docs, will build a copy of the docs for your PR
