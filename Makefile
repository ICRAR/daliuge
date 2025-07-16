.ONESHELL:
ENV_PREFIX=$(shell python -c "if __import__('pathlib').Path('.venv/bin/pip').exists(): print('%s/'% __import__('pathlib').Path('.venv/bin').absolute())")
USING_POETRY=$(shell grep "tool.poetry" pyproject.toml && echo "yes")

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep


.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@if [ "$(USING_POETRY)" ]; then poetry env info && exit; fi
	@echo "Running using $(ENV_PREFIX)"
	@$(ENV_PREFIX)python -V
	@$(ENV_PREFIX)python -m site

.PHONY: local
local:            ## Install the project using local enviroment
	@if [ "$(USING_POETRY)" ]; then poetry install && exit; fi
	@echo "Don't forget to run 'make virtualenv' if you got errors."
	@ pip install -e daliuge-common
	@ pip install -e daliuge-engine
	@ pip install -e daliuge-translator

.PHONY: docker-install
docker-install:	  ## Install using docker containers
	@if ! command -v docker; then echo "Docker is not available; please confirm it is installed." && exit; fi
	@  cd daliuge-common && ./build_common.sh dev && cd ..
	@  cd daliuge-engine && ./build_engine.sh devall && cd ..
	@  cd daliuge-translator && ./build_translator.sh devall && cd ..

.PHONY: docker-run
docker-run:	  ## Install using docker containers
	@if ! command -v docker; then echo "Docker is not available; please confirm it is installed." && exit; fi
	@  cd daliuge-engine && ./run_engine.sh dev && cd ..
	@  cd daliuge-translator && ./run_translator.sh dev && cd ..

.PHONY: docker-stop
docker-stop:	  ## Install using docker containers
	@if ! command -v docker; then echo "Docker is not available; please confirm it is installed." && exit; fi
	@  cd daliuge-engine && ./stop_engine.sh docker && cd ..
	@  cd daliuge-translator && ./stop_translator.sh && cd ..

.PHONY: lint
lint:             ## Run pylint
	$(ENV_PREFIX)pylint daliuge-common daliuge-translator daliuge-engine --fail-under=9 --fail-on=E

.PHONY: test
test: 		  ## Run tests and generate coverage report.
	@ pip install pytest
	@ pip install pytest-cov
	@ cd daliuge-translator
	@ $(env_prefix)py.test --cov --show-capture=no
	@ cd ../daliuge-engine
	@ $(ENV_PREFIX)py.test --cov --show-capture=no

.PHONY: clean
clean:            ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@#find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf .tox/
	@rm -rf docs/_build

.PHONY: virtualenv
virtualenv:       ## Create a virtual environment.
	@if [ "$(USING_POETRY)" ]; then poetry install && exit; fi
	@echo "creating virtualenv ..."
	@rm -rf .venv
	@python3 -m venv .venv
	@echo
	@echo "!!! Please run 'source .venv/bin/activate' to enable the environment !!!"

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@if ! grep -q "v$${TAG}" CHANGELOG.md; then echo "TAG version number must be added to CHANGELOG.md before committing." && exit; fi
	@echo "v$${TAG}" > daliuge-common/VERSION
	@echo "v$${TAG}" > daliuge-engine/VERSION
	@echo "v$${TAG}" > daliuge-translator/VERSION
	@git add daliuge-common/VERSION daliuge-engine/VERSION daliuge-translator/VERSION CHANGELOG.md
	@git commit -m "Release: version v$${TAG} 🚀"
	@echo "creating git tag : v$${TAG}"
	@git tag v$${TAG}
	@git push -u origin HEAD --tags
#	@echo "Github Actions will detect the new tag and release the new version."

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@echo $(ACTIVATE_VENV)
	@$(ACTIVATE_VENV)
	@$(ENV_PREFIX)pip install -r docs/requirements.txt
	@READTHEDOCS=True make -C docs html SPHINXOPTS="-W --keep-going"

# This Makefile has been based on the existing ICRAR/daliuge-component-template.
# __author__ = 'ICRAR'
# __repo__ = https://github.com/ICRAR/daliuge
# __sponsor__ = https://github.com/sponsors/ICRAR/
