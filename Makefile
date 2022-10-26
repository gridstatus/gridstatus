.PHONY: test
test:
	python -m pytest -s -vv -n auto isodata/ -m "not slow"

test-slow:
	python -m pytest -s -vv -n auto isodata/ -m "slow"

.PHONY: installdeps-dev
installdeps-dev:
	python -m pip install ".[dev]"
	pre-commit install

.PHONY: installdeps-test
installdeps-test:
	python -m pip install ".[test]"

.PHONY: lint
lint:
	isort --check-only isodata/
	black isodata/ -t py310 --check

.PHONY: lint-fix
lint-fix:
	black isodata/ -t py310
	isort isodata/

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
	$(eval PACKAGE=$(shell python -c "from pep517.meta import load; metadata = load('.'); print(metadata.version)"))
	tar -zxvf "dist/isodata-${PACKAGE}.tar.gz"
	mv "isodata-${PACKAGE}" unpacked
