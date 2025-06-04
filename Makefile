.PHONY: clean
clean:
	find . -name '*.pyo' -delete
	find . -name '*.pyc' -delete
	find . -name __pycache__ -delete
	find . -name '*~' -delete
	find . -name '.coverage.*' -delete

PYTEST_CMD := uv run pytest -s -vv -n auto --reruns 5 --reruns-delay 3 --durations=25
NOT_SLOW := -m "not slow"
UNIT_ONLY := -m "not integration"

.PHONY: test-base
test-base:
	$(PYTEST_CMD) gridstatus/tests/test_*.py --ignore=gridstatus/tests/source_specific/

.PHONY: test-aeso
test-aeso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_aeso.py

.PHONY: test-caiso
test-caiso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_caiso.py

.PHONY: test-ercot
test-ercot:
	$(PYTEST_CMD) $(NOT_SLOW) gridstatus/tests/source_specific/test_ercot.py gridstatus/tests/source_specific/test_ercot_api.py

.PHONY: test-isone
test-isone:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_isone.py gridstatus/tests/source_specific/test_isone_api.py

.PHONY: test-miso
test-miso:
	$(PYTEST_CMD) gridstatus/tests/source_specific/test_miso.py gridstatus/tests/source_specific/test_miso_api.py

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
	uv pip install vcrpy
	$(PYTEST_CMD) -m "slow"

.PHONY: test-unit
test-unit:
	uv pip install vcrpy
	$(PYTEST_CMD) $(UNIT_ONLY)

#TODO(kladar): Maybe gridstatus gets a CLI in the future for things like this.
#NB: This is for running a single test without having to fully specify the
#    path and the pytest command.
#    Usage: make test-one-off market=MARKET test=TEST_NAME
#    Example: make test-one-off market=pjm test=test_get_load_forecast_5_min_latest
.PHONY: test-one-off
test-one-off:
#NB: First two blocks are for error checking.
ifndef market
	$(error market parameter is required. Usage: make test-one-off market=MARKET test=TEST_NAME)
endif
ifndef test
	$(error test parameter is required. Usage: make test-one-off market=MARKET test=TEST_NAME)
endif
#NB: This puts the market in the right capitalization for the path. Ercot is not ALL CAPS like the other market test classes: TestErcot vs TestCAISO, etc.
ifeq ($(market),ercot)
	uv run pytest -vvv gridstatus/tests/source_specific/test_$(market).py::TestErcot::$(test)
else
	uv run pytest -vvv gridstatus/tests/source_specific/test_$(market).py::Test$(shell echo $(market) | tr '[:lower:]' '[:upper:]')::$(test)
endif

.PHONY: installdeps-dev
installdeps-dev:
	uv sync
	uv pip install vcrpy
	uv run pre-commit install

.PHONY: installdeps-test
installdeps-test:
	uv sync
	uv pip install vcrpy


.PHONY: installdeps-docs
installdeps-docs:
	uv sync

.PHONY: lint
lint:
	uv run ruff check gridstatus/
	uv run ruff format gridstatus/ --check

.PHONY: lint-fix
lint-fix:
	uv run ruff check gridstatus/ --fix
	uv run ruff format gridstatus/

.PHONY: upgradepip
upgradepip:
	uv pip install --upgrade pip

.PHONY: upgradebuild
upgradebuild:
	uv pip install --upgrade build

.PHONY: upgradesetuptools
upgradesetuptools:
	uv pip install --upgrade setuptools

.PHONY: package
package: upgradepip upgradebuild upgradesetuptools
	uv build

.PHONY: docs
docs: clean
	uv run make -C docs/ -e "SPHINXOPTS=-j auto" clean html

.PHONY: mypy-coverage
mypy-coverage:
	uv run mypy --html-report mypy_report gridstatus/
