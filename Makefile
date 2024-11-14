.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

PYTEST_CMD := poetry run pytest -s -vv -n auto --reruns 5 --reruns-delay 3 --durations=25
NOT_SLOW := -m "not slow"
UNIT_ONLY := -m "not slow and not integration"

.PHONY: test-base
test-base:
	$(PYTEST_CMD) gridstatus/tests/test_*.py --ignore=gridstatus/tests/source_specific/

.PHONY: test-caiso
test-caiso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_caiso.py

.PHONY: test-ercot
test-ercot:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_ercot.py gridstatus/tests/source_specific/test_ercot_api.py

.PHONY: test-isone
test-isone:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_isone.py

.PHONY: test-miso
test-miso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_miso.py

.PHONY: test-nyiso
test-nyiso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_nyiso.py

.PHONY: test-pjm
test-pjm:
	$(PYTEST_CMD) $(NOT_SLOW) gridstatus/tests/source_specific/test_pjm.py

.PHONY: test-spp
test-spp:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_spp.py

.PHONY: test-eia
test-eia:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_eia.py

.PHONY: test-ieso
test-ieso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_ieso.py

.PHONY: test-misc
test-misc:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_gridstatus.py gridstatus/tests/source_specific/test_lmp_config.py

.PHONY: test-cov
test-cov:
	$(PYTEST_CMD) $(NOT_SLOW) --cov=gridstatus --cov-config=./pyproject.toml --cov-report=xml:./coverage.xml

.PHONY: test-slow
test-slow:
	pip install vcrpy
	$(PYTEST_CMD) -m "slow"

.PHONY: test-unit
test-unit:
	pip install vcrpy
	$(PYTEST_CMD) $(UNIT_ONLY)

.PHONY: installdeps-dev
installdeps-dev:
	poetry install --all-extras
	pip install vcrpy
	poetry run pre-commit install

.PHONY: installdeps-test
installdeps-test:
	poetry run pip install vcrpy
	poetry install --all-extras

.PHONY: installdeps-docs
installdeps-docs:
	poetry install --all-extras

.PHONY: lint
lint:
	poetry run ruff check gridstatus/
	poetry run ruff format gridstatus/ --check

.PHONY: lint-fix
lint-fix:
	poetry run ruff check gridstatus/ --fix
	poetry run ruff format gridstatus/

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

.PHONY: mypy-coverage
mypy-coverage:
	poetry run mypy --html-report mypy_report gridstatus/
