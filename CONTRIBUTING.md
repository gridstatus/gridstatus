# Contributing

Thank you for considering contributing to our project! We are grateful for any time and effort you put into making this project better.

There are several ways you can contribute:

* Submitting bug reports and feature requests

* Submitting pull requests for code changes

* Adding documentation or examples

* Participating in discussions or providing feedback

Before contributing, please read this guide. If you have any questions, feel free to ask in our [Slack](https://join.slack.com/t/gridstatus/shared_invite/zt-1jk6vlzt2-Lzz4pdpjkJYVUJkynOiIvQ) or open a GitHub issue.

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

The best way to ensure everything is installed correctly by running running the tests. They should all pass.

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

## CI/CD

When you submit a PR, the following actions are automatically performed

1. GitHub Actions runs tests the test in all support python versions
2. GithHub Actions ensures the code is properly linted
3. Our documentation host, Read the Docs, will build a copy of the docs for your PR
