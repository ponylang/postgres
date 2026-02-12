config ?= release

PACKAGE := postgres
GET_DEPENDENCIES_WITH := corral fetch
CLEAN_DEPENDENCIES_WITH := corral clean
COMPILE_WITH := corral run -- ponyc

BUILD_DIR ?= build/$(config)
COVERAGE_DIR ?= build/coverage
SRC_DIR ?= $(PACKAGE)
EXAMPLES_DIR := examples
coverage_binary := $(COVERAGE_DIR)/$(PACKAGE)
tests_binary := $(BUILD_DIR)/$(PACKAGE)
docs_dir := build/$(PACKAGE)-docs

ifdef config
	ifeq (,$(filter $(config),debug release))
		$(error Unknown configuration "$(config)")
	endif
endif

ifeq ($(config),release)
	PONYC = $(COMPILE_WITH)
else
	PONYC = $(COMPILE_WITH) --debug
endif

ifeq (,$(filter $(MAKECMDGOALS),clean docs realclean start-pg-containers stop-pg-containers TAGS))
  ifeq ($(ssl), 3.0.x)
          SSL = -Dopenssl_3.0.x
  else ifeq ($(ssl), 1.1.x)
          SSL = -Dopenssl_1.1.x
  else ifeq ($(ssl), 0.9.0)
          SSL = -Dopenssl_0.9.0
  else
    $(error Unknown SSL version "$(ssl)". Must set using 'ssl=FOO')
  endif
endif

PONYC := $(PONYC) $(SSL)

SOURCE_FILES := $(shell find $(SRC_DIR) -name *.pony)
EXAMPLES := $(notdir $(shell find $(EXAMPLES_DIR)/* -type d))
EXAMPLES_SOURCE_FILES := $(shell find $(EXAMPLES_DIR) -name *.pony)
EXAMPLES_BINARIES := $(addprefix $(BUILD_DIR)/,$(EXAMPLES))

test: unit-tests integration-tests build-examples

unit-tests: $(tests_binary)
	$^ --exclude=integration/ --sequential

integration-tests: $(tests_binary)
	$^ --only=integration/ --sequential

$(tests_binary): $(SOURCE_FILES) | $(BUILD_DIR)
	$(GET_DEPENDENCIES_WITH)
	$(PONYC) -o $(BUILD_DIR) $(SRC_DIR)

build-examples: $(EXAMPLES_BINARIES)

$(EXAMPLES_BINARIES): $(BUILD_DIR)/%: $(SOURCE_FILES) $(EXAMPLES_SOURCE_FILES) | $(BUILD_DIR)
	$(GET_DEPENDENCIES_WITH)
	$(PONYC) -o $(BUILD_DIR) $(EXAMPLES_DIR)/$*

clean:
	$(CLEAN_DEPENDENCIES_WITH)
	rm -rf $(BUILD_DIR)
	rm -rf $(COVERAGE_DIR)

$(docs_dir): $(SOURCE_FILES)
	rm -rf $(docs_dir)
	$(GET_DEPENDENCIES_WITH)
	$(PONYC) --docs-public --pass=docs --output build $(SRC_DIR)

docs: $(docs_dir)

TAGS:
	ctags --recurse=yes $(SRC_DIR)

coverage: $(coverage_binary)
	kcov --include-pattern="/$(SRC_DIR)/" --exclude-pattern="/test/,_test.pony" $(COVERAGE_DIR) $(coverage_binary)

$(coverage_binary): $(SOURCE_FILES) | $(COVERAGE_DIR)
	$(GET_DEPENDENCIES_WITH)
	$(PONYC) --debug -o $(COVERAGE_DIR) $(SRC_DIR)

start-pg-containers:
	@docker run --name pg -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256" -v $(CURDIR)/.ci-dockerfiles/pg-ssl/init-md5-user.sh:/docker-entrypoint-initdb.d/init-md5-user.sh:ro -p 5432:5432 -d postgres:14.5
	@docker run --name pg-ssl -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256" -v $(CURDIR)/assets/test-cert.pem:/var/lib/postgresql/server.crt:ro -v $(CURDIR)/assets/test-key.pem:/var/lib/postgresql/server.key.orig:ro -v $(CURDIR)/.ci-dockerfiles/pg-ssl/init-md5-user.sh:/docker-entrypoint-initdb.d/init-md5-user.sh:ro -p 5433:5432 -d --entrypoint sh postgres:14.5 -c "cp /var/lib/postgresql/server.key.orig /var/lib/postgresql/server.key && chmod 600 /var/lib/postgresql/server.key && chown postgres:postgres /var/lib/postgresql/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key"

stop-pg-containers:
	@docker stop pg pg-ssl
	@docker rm pg pg-ssl

all: test

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(COVERAGE_DIR):
	mkdir -p $(COVERAGE_DIR)

.PHONY: all build-examples clean docs TAGS test coverage start-pg-containers stop-pg-containers
