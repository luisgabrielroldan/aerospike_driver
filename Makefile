DRIVER_COMPOSE ?= docker compose
SECURITY_USER ?= admin
SECURITY_PASSWORD ?= admin
PROFILE ?= unit

.PHONY: help \
	test coverage validate \
	test-unit test-coverage test-coverage-live test-coverage-all \
	test-ce test-cluster test-enterprise test-live test-all \
	deps deps-up deps-cluster-up deps-enterprise-up deps-all-up deps-down \
	wait-service

help:
	@printf '%s\n' \
		'test                 Run tests for PROFILE=unit|ce|cluster|enterprise|live|all (default: unit)' \
		'coverage             Run coverage for PROFILE=unit|live|all (default: unit)' \
		'validate             Run format, compile, credo, dialyzer, unit, and coverage checks' \
		'deps                 Start docker dependencies for PROFILE=ce|cluster|enterprise|all (default: ce)' \
		'test-unit            Compatibility shim for `make test PROFILE=unit`' \
		'test-coverage        Compatibility shim for `make coverage PROFILE=unit`' \
		'test-coverage-live   Compatibility shim for `make coverage PROFILE=live`' \
		'test-coverage-all    Compatibility shim for `make coverage PROFILE=all`' \
		'test-ce              Compatibility shim for `make test PROFILE=ce`' \
		'test-cluster         Compatibility shim for `make test PROFILE=cluster`' \
		'test-enterprise      Compatibility shim for `make test PROFILE=enterprise`' \
		'test-live            Compatibility shim for `make test PROFILE=live`' \
		'test-all             Compatibility shim for `make test PROFILE=all`' \
		'deps-up              Compatibility shim for `make deps PROFILE=ce`' \
		'deps-cluster-up      Compatibility shim for `make deps PROFILE=cluster`' \
		'deps-enterprise-up   Compatibility shim for `make deps PROFILE=enterprise`' \
		'deps-all-up          Compatibility shim for `make deps PROFILE=all`' \
		'deps-down            Stop all driver docker stacks'

test:
	@case "$(PROFILE)" in \
		unit) mix test.unit ;; \
		ce) mix test.integration.ce ;; \
		cluster) mix test.integration.cluster ;; \
		enterprise) mix test.integration.enterprise ;; \
		live) mix test.live ;; \
		all) mix test.integration.all ;; \
		*) echo "Unsupported PROFILE for test: $(PROFILE). Use unit|ce|cluster|enterprise|live|all."; exit 1 ;; \
	esac

coverage:
	@ulimit -n 4096; case "$(PROFILE)" in \
		unit) mix test.coverage ;; \
		live) mix test.coverage.live ;; \
		all) mix test.coverage.all ;; \
		*) echo "Unsupported PROFILE for coverage: $(PROFILE). Use unit|live|all."; exit 1 ;; \
	esac

validate:
	mix validate

test-unit:
	@$(MAKE) test PROFILE=unit

test-coverage:
	@$(MAKE) coverage PROFILE=unit

test-coverage-live:
	@$(MAKE) coverage PROFILE=live

test-coverage-all:
	@$(MAKE) coverage PROFILE=all

test-ce:
	@$(MAKE) test PROFILE=ce

test-cluster:
	@$(MAKE) test PROFILE=cluster

test-enterprise:
	@$(MAKE) test PROFILE=enterprise

test-live:
	@$(MAKE) test PROFILE=live

test-all:
	@$(MAKE) test PROFILE=all

deps:
	@case "$(PROFILE)" in \
		ce) $(MAKE) deps-up ;; \
		cluster) $(MAKE) deps-cluster-up ;; \
		enterprise) $(MAKE) deps-enterprise-up ;; \
		all) $(MAKE) deps-all-up ;; \
		*) echo "Unsupported PROFILE for deps: $(PROFILE). Use ce|cluster|enterprise|all."; exit 1 ;; \
	esac

deps-up:
	@$(DRIVER_COMPOSE) up -d aerospike
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=1
	@echo "Driver CE single-node ready on localhost:3000"

deps-cluster-up:
	@$(DRIVER_COMPOSE) --profile cluster up -d aerospike aerospike2 aerospike3
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike2 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike3 EXPECTED_SIZE=3
	@echo "Driver cluster ready on localhost:3000, :3010, :3020"

deps-enterprise-up:
	@$(DRIVER_COMPOSE) --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki aerospike-ee-security aerospike-ee-security-tls
	@$(MAKE) wait-service SERVICE=aerospike-ee EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-tls EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-pki EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security-tls EXPECTED_SIZE=1
	@echo "Driver enterprise stack ready on localhost:3100, :3200, :4333, :4334, :4433"

deps-all-up: deps-cluster-up deps-enterprise-up
	@echo "All documented driver test dependencies are ready"

deps-down:
	@$(DRIVER_COMPOSE) --profile cluster --profile enterprise down
	@echo "Driver docker stacks stopped"

wait-service:
	@echo "Waiting for $(SERVICE) (expected cluster size $(EXPECTED_SIZE))..."
	@for i in $$(seq 1 45); do \
		if [ "$(SERVICE)" = "aerospike-ee-security" ] || [ "$(SERVICE)" = "aerospike-ee-security-tls" ]; then \
			PROBE_CMD="asinfo -U $(SECURITY_USER) -P $(SECURITY_PASSWORD) -v 'cluster-stable:size=$(EXPECTED_SIZE);ignore-migrations=true'"; \
		else \
			PROBE_CMD="asinfo -v 'cluster-stable:size=$(EXPECTED_SIZE);ignore-migrations=true'"; \
		fi; \
		OUTPUT=$$(docker exec $(SERVICE) sh -lc "$$PROBE_CMD" 2>/dev/null || true); \
		case "$$OUTPUT" in \
			ERROR*|'') ;; \
			*) echo "$(SERVICE) is stable ($$OUTPUT)"; exit 0 ;; \
		esac; \
		if [ $$i -eq 45 ]; then \
			echo "$(SERVICE) did not reach stable cluster size $(EXPECTED_SIZE) in time"; \
			docker logs --tail 100 $(SERVICE) || true; \
			exit 1; \
		fi; \
		sleep 2; \
	done
