.PHONY: test
test:
	pytest -s -vv isodata/

.PHONY: installdeps-dev
installdeps-dev:
	python -m pip install ".[dev]"

.PHONY: lint
lint:
	python -m isort --check-only isodata
	python -m black isodata -t py310 --check

.PHONY: lint-fix
lint-fix:
	python -m black isodata -t py310
	python -m isort isodata

.PHONY: upgradepip
upgradepip:
	python -m pip install --upgrade pip

.PHONY: upgradebuild
upgradebuild:
	python -m pip install --upgrade build

.PHONY: package
package: upgradepip upgradebuild
	python -m build
	$(eval PACKAGE=$(shell python -c "from pep517.meta import load; metadata = load('.'); print(metadata.version)"))
	tar -zxvf "dist/isodata-${PACKAGE}.tar.gz"
	mv "isodata-${PACKAGE}" unpacked
