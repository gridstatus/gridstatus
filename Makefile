.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

PYTEST_CMD := poetry run pytest -s -vv gridstatus/ -n auto --reruns 5 --reruns-delay 3
NOT_SLOW_AND_NOT_REQUIRES_ERCOT_AUTH := -m "not slow and not requires_ercot_auth"

.PHONY: test
test:
	$(PYTEST_CMD) $(NOT_SLOW_AND_NOT_REQUIRES_ERCOT_AUTH)

.PHONY: test-cov
test-cov:
	$(PYTEST_CMD) $(NOT_SLOW_AND_NOT_REQUIRES_ERCOT_AUTH) --cov=gridstatus --cov-config=./pyproject.toml --cov-report=xml:./coverage.xml

.PHONY: test-ercot-api
test-ercot-api:
	$(PYTEST_CMD) -m "not slow and requires_ercot_auth"

.PHONY: test-slow
test-slow:
	$(PYTEST_CMD) -m "slow and not requires_ercot_auth"

.PHONY: installdeps-dev
installdeps-dev:
	poetry install --all-extras
	poetry run pre-commit install

.PHONY: installdeps-test
installdeps-test:
	poetry install --all-extras

.PHONY: installdeps-docs
installdeps-docs:
	poetry install --all-extras

.PHONY: lint
lint:
	poetry run ruff gridstatus/
	poetry run black gridstatus/ --check

.PHONY: lint-fix
lint-fix:
	poetry run ruff gridstatus/ --fix
	poetry run black gridstatus/

.PHONY: upgradepip
upgradepip:
	poetry run python -m pip install --upgrade pip

.PHONY: upgradebuild
upgradebuild:
	poetry run python -m pip install --upgrade build

.PHONY: upgradesetuptools
upgradesetuptools:
	poetry run python -m pip install --upgrade setuptools

.PHONY: package
package: upgradepip upgradebuild upgradesetuptools
	poetry build

.PHONY: docs
docs: clean
	poetry run make -C docs/ -e "SPHINXOPTS=-j auto" clean html
