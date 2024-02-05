.PHONY: clean
clean:
	poetry run find . -name '*.pyo' -delete
	poetry run find . -name '*.pyc' -delete
	poetry run find . -name __pycache__ -delete
	poetry run find . -name '*~' -delete
	poetry run find . -name '.coverage.*' -delete

PYTEST_CMD := poetry run pytest -s -vv gridstatus/ -n auto
NOT_SLOW := -m "not slow" --reruns 5 --reruns-delay 3

.PHONY: test
test:
	$(PYTEST_CMD) $(NOT_SLOW)

.PHONY: test-cov
test-cov:
	$(PYTEST_CMD) $(NOT_SLOW) --cov=gridstatus --cov-config=./pyproject.toml --cov-report=xml:./coverage.xml

.PHONY: test-slow
test-slow:
	$(PYTEST_CMD) -m "slow"

.PHONY: installdeps-dev
installdeps-dev:
	poetry install --extras "dev"
	poetry run pre-commit install

.PHONY: installdeps-test
installdeps-test:
	poetry install --extras "test"

.PHONY: installdeps-docs
installdeps-docs:
	poetry install --extras "docs"

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
	$(eval PACKAGE=$(shell poetry run python -c 'import setuptools; setuptools.setup()' --version))
	tar -zxvf "dist/gridstatus-${PACKAGE}.tar.gz"
	mv "gridstatus-${PACKAGE}" unpacked

.PHONY: docs
docs: clean
	make -C docs/ -e "SPHINXOPTS=-j auto" clean html
