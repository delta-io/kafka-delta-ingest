.DEFAULT_GOAL := help

CARGO:=$(shell which cargo)
APP=target/debug/placementd-web
SOURCES=$(shell find . -type f -iname '*.rs')

$(APP): Cargo.toml $(SOURCES)
	$(CARGO) build

.PHONY: check
check: Cargo.toml $(SOURCES)
	$(CARGO) fmt
	$(CARGO) test --features azure,s3

.PHONY: docker
docker: Dockerfile ## Build the docker image
	docker build -t kafka-delta-ingest .


.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
