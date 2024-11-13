PACKAGE = aave_point_tracker
RUNINENV = . .venv/bin/activate
CHECKENV = python -c \
	"import sys ; sys.exit(0) if sys.prefix == sys.base_prefix else sys.exit(1)" \
	&& echo Activate venv && $(RUNINENV) || echo Already in venv


.PHONY: setup-core
setup-core:
	make clean
	python -m venv .venv
	$(RUNINENV) \
	&& python -m pip cache remove $(PACKAGE) \
	&& python -m pip install --upgrade pip \
	&& python -m pip install --upgrade build

.PHONY: setup
setup:
	make setup-core
	$(RUNINENV) && python -m pip install .

.PHONY: setup-dev
setup-dev:
	make setup-core
	$(RUNINENV) \
	&& python -m pip install --editable '.[dev]' \
	&& python -m mypy --install-types --non-interactive	

.PHONY: run
run:
	$(CHECKENV) && python src/$(PACKAGE)/main.py

.PHONY: run-dev
run-dev:
	make test
	make run

.PHONY: server
server:
	$(CHECKENV) && fastapi run src/$(PACKAGE)/app.py

.PHONY: test
test:
	$(CHECKENV) && python -m pytest tests \
	--cov-report term-missing:skip-covered --cov=$(PACKAGE)

.PHONY: clean
clean:
	deactivate ; \
	rm -rf .venv build dist \
	&& find . -name '*.egg-info' -exec rm -r {} + \
	&& pyclean . \	