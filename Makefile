# The binary to build (just the basename).
MODULE := kafkamysql

# Where to push the docker image.
REGISTRY ?= sgioldasis

IMAGE := $(REGISTRY)/$(MODULE)

# This version-strategy uses git tags to set the version string
TAG := $(shell git describe --tags --always --dirty)

BLUE='\033[0;34m'
NC='\033[0m' # No Color

infra-up:
	@echo "\n${BLUE}Starting the infrastructure...${NC}\n"
	@docker-compose -f docker-compose.infra.yml up -d

infra-down:
	@echo "\n${BLUE}Stopping the infrastructure...${NC}\n"
	@docker-compose -f docker-compose.infra.yml down

install:
	@pip install -r requirements.txt

config:
	@cp kafkamysql/config.prod.template.yml kafkamysql/config.prod.yml

init-db:
	@cd kafkamysql ; python db_init.py prod ; cd ..

run:
	@python -m $(MODULE)

test-dep:
	@export TEST_ENV='test'; cd kafkamysql ; python db_init.py test ; cd .. ; pytest

sleep:
	@sleep 30

test: infra-up sleep test-dep infra-down

test-docker:
	@sleep 20 ; cd kafkamysql ; python db_init.py docker ; cd .. ; pytest

docker-test:
	@docker-compose up --build --abort-on-container-exit ; docker-compose down

.PHONY: clean image-clean build-prod push test

clean:
	rm -rf .pytest_cache tests/__pycache__ tests/__pytest_cache__ kafkamysql/__pycache__ kafkamysql/__pytest_cache__ .coverage coverage.xml htmlcov
