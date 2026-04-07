TLS_FIXTURES_DIR ?= test/support/fixtures/tls
TLS_DAYS ?= 3650

.PHONY: tls-fixtures tls-fixtures-clean test.tls \
	deps-up deps-cluster-up deps-enterprise-up deps-all-up deps-down

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
	@echo "Dependencies ready: localhost:3000"

# Multi-node cluster dependencies (cluster integration tests).
deps-cluster-up:
	@docker compose --profile cluster up -d aerospike aerospike2 aerospike3
	@echo "Cluster dependencies ready: localhost:3000, :3010, :3020"

# Enterprise dependencies (enterprise/TLS tests).
deps-enterprise-up: tls-fixtures
	@docker compose --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki
	@echo "Enterprise dependencies ready: localhost:3100 (EE), :4333 (TLS), :4334 (mTLS)"

# Full dependency stack for full-suite testing.
deps-all-up: tls-fixtures
	@docker compose --profile cluster --profile enterprise up -d
	@echo "All dependencies ready (single-node, cluster, enterprise)"

deps-down:
	@docker compose --profile cluster --profile enterprise down
