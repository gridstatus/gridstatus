.PHONY: test
test:
	python -m pytest -s -vv gridstatus/ -m "not slow" -n auto  --reruns 5 --reruns-delay 3

.PHONY: test-slow
test-slow:
	python -m pytest -s -vv gridstatus/ -m "slow" -n auto

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
	isort --check-only gridstatus/
	black gridstatus/ -t py310 --check

.PHONY: lint-fix
lint-fix:
	black gridstatus/ -t py310
	isort gridstatus/

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
	tar -zxvf "dist/gridstatus-${PACKAGE}.tar.gz"
	mv "gridstatus-${PACKAGE}" unpacked
