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

lint:
	@echo "\n${BLUE}Running Pylint against source and test files...${NC}\n"
	@pylint --rcfile=setup.cfg **/*.py
	@echo "\n${BLUE}Running Flake8 against source and test files...${NC}\n"
	@flake8
	@echo "\n${BLUE}Running Bandit against source files...${NC}\n"
	@bandit -r --ini setup.cfg

# Example: make build-prod VERSION=1.0.0
build-prod:
	@echo "\n${BLUE}Building Production image with labels:\n"
	@echo "name: $(MODULE)"
	@echo "version: $(VERSION)${NC}\n"
	@sed                                     \
	    -e 's|{NAME}|$(MODULE)|g'            \
	    -e 's|{VERSION}|$(VERSION)|g'        \
	    prod.Dockerfile | docker build -t $(IMAGE):$(VERSION) -f- .


build-dev:
	@echo "\n${BLUE}Building Development image with labels:\n"
	@echo "name: $(MODULE)"
	@echo "version: $(TAG)${NC}\n"
	@sed                                 \
	    -e 's|{NAME}|$(MODULE)|g'        \
	    -e 's|{VERSION}|$(TAG)|g'        \
	    dev.Dockerfile | docker build -t $(IMAGE):$(TAG) -f- .

# Example: make shell CMD="-c 'date > datefile'"
shell: build-dev
	@echo "\n${BLUE}Launching a shell in the containerized build environment...${NC}\n"
		@docker run                                                 \
			-ti                                                     \
			--rm                                                    \
			--entrypoint /bin/bash                                  \
			-u $$(id -u):$$(id -g)                                  \
			$(IMAGE):$(TAG)										    \
			$(CMD)

# Example: make push VERSION=0.0.2
push: build-prod
	@echo "\n${BLUE}Pushing image to GitHub Docker Registry...${NC}\n"
	@docker push $(IMAGE):$(VERSION)

version:
	@echo $(TAG)

.PHONY: clean image-clean build-prod push test

clean:
	rm -rf .pytest_cache tests/__pycache__ tests/__pytest_cache__ kafkamysql/__pycache__ kafkamysql/__pytest_cache__ .coverage coverage.xml htmlcov

docker-clean:
	@docker system prune -f --filter "label=name=$(MODULE)"