#################################################################################
#
# Makefile to build the project
#
#################################################################################


PROJECT_NAME = NC-project
REGION = eu-west-2
PYTHON_INTERPRETER = python3
WD=$(shell pwd)
PYTHONPATH=".:./src:./test:./spikes"
SHELL := /bin/bash
PROFILE = default
PIP:=pip
PYTEST_OPTS = -vvvv
PYTEST_COV = --cov=src --cov-fail-under=90 --no-cov-on-fail --cov-report=term-missing
TRACK=.make_trackers
VENV=venv
SITE_PACKAGES=$(TRACK)/packages
PSQL_ENV=.env.ini
TF_DIR=Terraform

## Run all checks
run-checks: hook $(SITE_PACKAGES) notices unit-tests run-security run-flake

notices: unfrozen

## Create python interpreter environment.
$(VENV):
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    $(PYTHON_INTERPRETER) -m venv $(VENV); \
	)


$(TRACK)/packages : requirements.txt $(VENV)
	$(call execute_in_env, $(PIP) install -r requirements.txt)
	touch $(TRACK)/packages

unfrozen:
	@$(call execute_in_env, ./scripts/unfrozen.sh)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# Run terraform actions in Terraform directory
define terraform_action
	cd $(TF_DIR) && $1
endef

################################################################################################################
# Set Up
## local testing database
init-db $(PSQL_ENV):
	# Why does make have to parse like this?
	if [[ ! -f $(PSQL_ENV) ]]; then \
		./scripts/psqlcreds.sh $(PSQL_ENV) ;\
	fi;
	psql -f ./test/test_extract_db/subset_test_db_simple.sql

hook:
	@ln -sf $(realpath -s pre-commit.sh) .git/hooks/pre-commit


## Install flake8
dev-setup: init-db
	$(call execute_in_env, $(PIP) install flake8)
	$(call execute_in_env, $(PIP) install pytest)
	$(call execute_in_env, $(PIP) install pytest-cov)
	$(call execute_in_env, $(PIP) install bandit)
	$(call execute_in_env, $(PIP) install safety)


## Run the flake8 code check
run-flake:
	$(call execute_in_env, flake8 src test)

run-bandit:
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

$(TRACK)/safety : requirements.txt
	$(call execute_in_env, safety check -r ./requirements.txt)
	touch $(TRACK)/safety

## Run all the unit tests
unit-tests:
	$(call execute_in_env, PYTHONPATH="${PYTHONPATH}" pytest ${PYTEST_OPTS} ${PYTEST_COV} $(ARGS) test/*.py)

run-security: run-bandit $(TRACK)/safety

init: $(VENV) dev-setup init-db
	mkdir -p $(TRACK)
	make $(SITE_PACKAGES)
actions-init: $(VENV)
	mkdir -p $(TRACK)
	make $(SITE_PACKAGES)

clean:
	@echo "erasing setup"
	rm -drf $(VENV)
	rm .env.ini
	echo "drop database totesys_test_subset" | psql

.PHONY: clean init actions-init unit-tests run-security run-bandit run-flake dev-setup hook init-db unfrozen notices run-checks
