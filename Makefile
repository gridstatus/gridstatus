.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

.PHONY: test
test:
	python -m pytest -s -vv gridstatus/ -m "not slow" -n auto  --reruns 5 --reruns-delay 3 2>&1 >/dev/null

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
	black gridstatus/ -t py311 --check

.PHONY: lint-fix
lint-fix:
	black gridstatus/ -t py311
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

.PHONY: docs
docs: clean
	make -C docs/ -e "SPHINXOPTS=-j auto" clean html
