TLS_FIXTURES_DIR ?= test/support/fixtures/tls
TLS_DAYS ?= 3650
AEROSPIKE_HOST ?= 127.0.0.1
AEROSPIKE_PORT ?= 3000
AEROSPIKE_EE_HOST ?= 127.0.0.1
AEROSPIKE_EE_PORT ?= 3100
AEROSPIKE_SECURITY_EE_HOST ?= 127.0.0.1
AEROSPIKE_SECURITY_EE_PORT ?= 3200
AEROSPIKE_SECURITY_EE_USER ?= admin
AEROSPIKE_SECURITY_EE_PASSWORD ?= admin

.PHONY: tls-fixtures tls-fixtures-clean test.tls \
	deps-up deps-cluster-up deps-enterprise-up deps-all-up deps-down \
	test-unit-ci test-integration-ci test-enterprise-ci test-cluster-ci

tls-fixtures:
	@command -v openssl >/dev/null 2>&1 || (echo "openssl is required" && exit 1)
	@mkdir -p "$(TLS_FIXTURES_DIR)"
	@rm -f "$(TLS_FIXTURES_DIR)"/*.key "$(TLS_FIXTURES_DIR)"/*.csr "$(TLS_FIXTURES_DIR)"/*.crt "$(TLS_FIXTURES_DIR)"/*.srl "$(TLS_FIXTURES_DIR)"/*.cnf
	@openssl genrsa -out "$(TLS_FIXTURES_DIR)/ca.key" 4096
	@openssl req -x509 -new -nodes -key "$(TLS_FIXTURES_DIR)/ca.key" -sha256 -days "$(TLS_DAYS)" -subj "/CN=Aerospike Test CA" -out "$(TLS_FIXTURES_DIR)/ca.crt"
	@openssl genrsa -out "$(TLS_FIXTURES_DIR)/server.key" 2048
	@printf '%s\n' '[req]' 'distinguished_name=req_distinguished_name' '[req_distinguished_name]' '[v3_req]' 'subjectAltName=DNS:localhost,IP:127.0.0.1' 'extendedKeyUsage=serverAuth' > "$(TLS_FIXTURES_DIR)/server-ext.cnf"
	@openssl req -new -key "$(TLS_FIXTURES_DIR)/server.key" -subj "/CN=localhost" -out "$(TLS_FIXTURES_DIR)/server.csr"
	@openssl x509 -req -in "$(TLS_FIXTURES_DIR)/server.csr" -CA "$(TLS_FIXTURES_DIR)/ca.crt" -CAkey "$(TLS_FIXTURES_DIR)/ca.key" -CAcreateserial -out "$(TLS_FIXTURES_DIR)/server.crt" -days "$(TLS_DAYS)" -sha256 -extfile "$(TLS_FIXTURES_DIR)/server-ext.cnf" -extensions v3_req
	@openssl genrsa -out "$(TLS_FIXTURES_DIR)/client.key" 2048
	@printf '%s\n' '[req]' 'distinguished_name=req_distinguished_name' '[req_distinguished_name]' '[v3_req]' 'extendedKeyUsage=clientAuth' > "$(TLS_FIXTURES_DIR)/client-ext.cnf"
	@openssl req -new -key "$(TLS_FIXTURES_DIR)/client.key" -subj "/CN=aerospike-test-client" -out "$(TLS_FIXTURES_DIR)/client.csr"
	@openssl x509 -req -in "$(TLS_FIXTURES_DIR)/client.csr" -CA "$(TLS_FIXTURES_DIR)/ca.crt" -CAkey "$(TLS_FIXTURES_DIR)/ca.key" -CAcreateserial -out "$(TLS_FIXTURES_DIR)/client.crt" -days "$(TLS_DAYS)" -sha256 -extfile "$(TLS_FIXTURES_DIR)/client-ext.cnf" -extensions v3_req
	@chmod 600 "$(TLS_FIXTURES_DIR)/ca.key" "$(TLS_FIXTURES_DIR)/server.key" "$(TLS_FIXTURES_DIR)/client.key"
	@echo "TLS fixtures generated in $(TLS_FIXTURES_DIR)"

tls-fixtures-clean:
	@rm -rf "$(TLS_FIXTURES_DIR)"
	@echo "Removed $(TLS_FIXTURES_DIR)"

test.tls: tls-fixtures
	@mix test test/aerospike/connection_tls_test.exs

# Single-node dependencies (unit/property/basic integration).
deps-up:
	@docker compose up -d aerospike
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=1
	@echo "Dependencies ready: localhost:3000"

# Multi-node cluster dependencies (cluster integration tests).
deps-cluster-up:
	@docker compose --profile cluster up -d aerospike aerospike2 aerospike3
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike2 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike3 EXPECTED_SIZE=3
	@echo "Cluster dependencies ready: localhost:3000, :3010, :3020"

# Enterprise dependencies (enterprise/TLS tests).
deps-enterprise-up: tls-fixtures
	@docker compose --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki aerospike-ee-security
	@$(MAKE) wait-service SERVICE=aerospike-ee EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-tls EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-pki EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security EXPECTED_SIZE=1
	@$(MAKE) ensure-roster SERVICE=aerospike-ee
	@$(MAKE) ensure-roster SERVICE=aerospike-ee-tls
	@$(MAKE) ensure-roster SERVICE=aerospike-ee-pki
	@echo "Enterprise dependencies ready: localhost:3100 (EE), :3200 (security), :4333 (TLS), :4334 (mTLS)"

# Full dependency stack for full-suite testing.
deps-all-up: tls-fixtures
	@docker compose --profile cluster --profile enterprise up -d
	@$(MAKE) wait-service SERVICE=aerospike1 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike2 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike3 EXPECTED_SIZE=3
	@$(MAKE) wait-service SERVICE=aerospike-ee EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-tls EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-pki EXPECTED_SIZE=1
	@$(MAKE) wait-service SERVICE=aerospike-ee-security EXPECTED_SIZE=1
	@$(MAKE) ensure-roster SERVICE=aerospike-ee
	@$(MAKE) ensure-roster SERVICE=aerospike-ee-tls
	@$(MAKE) ensure-roster SERVICE=aerospike-ee-pki
	@echo "All dependencies ready (single-node, cluster, enterprise)"

deps-down:
	@docker compose --profile cluster --profile enterprise down

wait-service:
	@echo "Waiting for $(SERVICE) (expected cluster size $(EXPECTED_SIZE))..."
	@for i in $$(seq 1 45); do \
		if [ "$(SERVICE)" = "aerospike-ee-security" ]; then \
			PROBE_CMD="asinfo -U $(AEROSPIKE_SECURITY_EE_USER) -P $(AEROSPIKE_SECURITY_EE_PASSWORD) -v 'cluster-stable:size=$(EXPECTED_SIZE);ignore-migrations=true'"; \
		else \
			PROBE_CMD="asinfo -v 'cluster-stable:size=$(EXPECTED_SIZE);ignore-migrations=true'"; \
		fi; \
		OUTPUT=$$(docker exec $(SERVICE) sh -lc "$$PROBE_CMD" 2>/dev/null || true); \
		case "$$OUTPUT" in \
			ERROR*|'') ;; \
			*) echo "$(SERVICE) is stable (cluster key $$OUTPUT)"; exit 0 ;; \
		esac; \
		if [ $$i -eq 45 ]; then \
			echo "$(SERVICE) did not reach stable cluster size $(EXPECTED_SIZE) in time"; \
			docker exec $(SERVICE) sh -lc "$$PROBE_CMD" || true; \
			docker logs --tail 100 $(SERVICE) || true; \
			exit 1; \
		fi; \
		sleep 2; \
	done

ensure-roster:
	@echo "Checking roster on $(SERVICE)..."
	@for i in $$(seq 1 15); do \
		ROSTER=$$(docker exec $(SERVICE) asinfo -v 'roster:namespace=test' 2>/dev/null || true); \
		if echo "$$ROSTER" | grep -q 'roster=' && ! echo "$$ROSTER" | grep -q 'roster=null'; then \
			echo "$(SERVICE): roster OK"; \
			exit 0; \
		fi; \
		if echo "$$ROSTER" | grep -q 'roster=null'; then \
			NODE_ID=$$(docker exec $(SERVICE) asinfo -v 'node' 2>/dev/null | tr -d '[:space:]'); \
			echo "$(SERVICE): roster=null, setting roster -> $$NODE_ID"; \
			docker exec $(SERVICE) asinfo -v "roster-set:namespace=test;nodes=$$NODE_ID" || true; \
			docker exec $(SERVICE) asinfo -v 'recluster:' || true; \
		fi; \
		sleep 2; \
	done; \
	echo "$(SERVICE): roster not ready after 30s"; \
	docker exec $(SERVICE) asinfo -v 'roster:namespace=test' || true; \
	exit 1

test-unit-ci: tls-fixtures
	@mix test --include property

test-integration-ci:
	@AEROSPIKE_HOST=$(AEROSPIKE_HOST) AEROSPIKE_PORT=$(AEROSPIKE_PORT) \
		mix test --include integration

test-enterprise-ci:
	@AEROSPIKE_EE_HOST=$(AEROSPIKE_EE_HOST) AEROSPIKE_EE_PORT=$(AEROSPIKE_EE_PORT) \
	AEROSPIKE_SECURITY_EE_HOST=$(AEROSPIKE_SECURITY_EE_HOST) AEROSPIKE_SECURITY_EE_PORT=$(AEROSPIKE_SECURITY_EE_PORT) \
	AEROSPIKE_SECURITY_EE_USER=$(AEROSPIKE_SECURITY_EE_USER) AEROSPIKE_SECURITY_EE_PASSWORD=$(AEROSPIKE_SECURITY_EE_PASSWORD) \
		mix test --include enterprise --include tls_stack --include security

test-cluster-ci:
	@AEROSPIKE_HOST=$(AEROSPIKE_HOST) AEROSPIKE_PORT=$(AEROSPIKE_PORT) \
		mix test --include cluster
