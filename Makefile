SPIKE_COMPOSE ?= docker compose
CLUSTER_COMPOSE ?= docker compose -f ../aerospike_driver/docker-compose.yml
SECURITY_USER ?= admin
SECURITY_PASSWORD ?= admin

.PHONY: help \
	test-unit test-coverage validate \
	test-ce test-cluster test-enterprise test-live test-all \
	deps-up deps-cluster-up deps-enterprise-up deps-all-up deps-down \
	wait-service

help:
	@printf '%s\n' \
		'test-unit            Run deterministic unit/doctest suite' \
		'test-coverage        Run deterministic suite with coverage gate' \
		'validate             Run format, compile, credo, unit, and coverage checks' \
		'test-ce              Run live CE single-node integration suite' \
		'test-cluster         Run live 3-node cluster integration suite' \
		'test-enterprise      Run live enterprise integration suite' \
		'test-live            Run CE + cluster live suites' \
		'test-all             Run all live suites' \
		'deps-up              Start spike CE single-node docker dependency' \
		'deps-cluster-up      Start reference 3-node cluster docker dependency' \
		'deps-enterprise-up   Start spike enterprise docker dependency stack' \
		'deps-all-up          Start all docker dependencies used by the spike test matrix' \
		'deps-down            Stop both spike and reference docker stacks'

test-unit:
	mix test.unit

test-coverage:
	mix test.coverage

validate:
	mix validate

test-ce:
	mix test.integration.ce

test-cluster:
	mix test.integration.cluster

test-enterprise:
	mix test.integration.enterprise

test-live:
	mix test.live

test-all:
	mix test.integration.all

deps-up:
	@$(SPIKE_COMPOSE) up -d aerospike
	@$(MAKE) wait-service SERVICE=aerospike-driver EXPECTED_SIZE=1
	@echo "Spike CE single-node ready on localhost:3000"

deps-cluster-up:
	@$(CLUSTER_COMPOSE) --profile cluster up -d aerospike aerospike2 aerospike3
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike2 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike3 EXPECTED_SIZE=3
	@echo "Reference cluster ready on localhost:3000, :3010, :3020"

deps-enterprise-up:
	@$(SPIKE_COMPOSE) --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki aerospike-ee-security aerospike-ee-security-tls
	@$(MAKE) wait-service SERVICE=aerospike-ee EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-tls EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-pki EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security-tls EXPECTED_SIZE=1
	@echo "Spike enterprise stack ready on localhost:3100, :3200, :4333, :4334, :4433"

deps-all-up: deps-up deps-cluster-up deps-enterprise-up
	@echo "All documented spike test dependencies are ready"

deps-down:
	@$(SPIKE_COMPOSE) --profile enterprise down
	@$(CLUSTER_COMPOSE) --profile cluster down
	@echo "Spike and reference docker stacks stopped"

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
