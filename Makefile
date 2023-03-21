.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

PYTEST_CMD := python -m pytest -s -vv gridstatus/ -n auto
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
	python -m pip install ".[dev]"
	pre-commit install

.PHONY: installdeps-test
installdeps-test:
	python -m pip install ".[test]"

.PHONY: installdeps-docs
installdeps-docs:
	python -m pip install ".[docs]"

.PHONY: lint
lint:
	ruff gridstatus/
	black gridstatus/ --check

.PHONY: lint-fix
lint-fix:
	ruff gridstatus/ --fix
	black gridstatus/

.PHONY: upgradepip
upgradepip:
	python -m pip install --upgrade pip

.PHONY: upgradebuild
upgradebuild:
	python -m pip install --upgrade build

.PHONY: upgradesetuptools
upgradesetuptools:
	python -m pip install --upgrade setuptools

.PHONY: package
package: upgradepip upgradebuild upgradesetuptools
	python -m build
	$(eval PACKAGE=$(shell python -c 'import setuptools; setuptools.setup()' --version))
	tar -zxvf "dist/gridstatus-${PACKAGE}.tar.gz"
	mv "gridstatus-${PACKAGE}" unpacked

.PHONY: docs
docs: clean
	make -C docs/ -e "SPHINXOPTS=-j auto" clean html
