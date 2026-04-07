# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Added tuple key input support at public API boundaries: callers can now pass `{namespace, set, user_key}` anywhere `%Aerospike.Key{}` is accepted in single-key facade operations, batch read/exists key lists, and `Aerospike.Batch` constructors.
- Added `Aerospike.Key.coerce!/1` plus `Aerospike.Key.key_input()` to normalize public key inputs to `%Aerospike.Key{}` internally.
- Added tuple-key coverage in facade, batch, and key unit tests.

### Changed
- Updated API docs for `Aerospike`, `Aerospike.Batch`, and `Aerospike.Key` to document tuple-key usage and clarify that digest-only flows should use `key_digest/3`.

## [0.1.0] - 2026-04-07

### Added
- Added GeoJSON wire encoding/decoding with proper binary layout (flags + ncells + cells + JSON)
- Added `{:geojson, json}` tuple representation for GeoJSON bin values
- Added transaction guide (`guides/transactions.md`)
- Added secondary indexes guide (`guides/secondary-indexes.md`)
- Added UDF guide (`guides/udfs.md`)
- Added SC-namespace query integration test
- Added transaction integration test
- Added demo examples for GeoJSON queries, query aggregation, concurrent transactions, and UDFs

### Fixed
- Fixed SC-namespace query streaming — handle zero-length frames and terminal result codes from server
- Fixed `partition_unavailable` handling in scan/query streams (skip instead of terminate)
- Fixed transaction commit to handle `:verified` state (skip re-verification)
- Fixed `:filtered_out` result code treated as fatal in scan response last-frame check
- Fixed flaky ETS named-table collision in `CRUDTxnTest` setup
- Fixed `max_records_fields/2` missing fallback clause for non-positive query limits

### Changed
- Refactored scan response RC dispatch into `stream_terminal_rc?` and `partition_skip_rc?` for clarity
- Refactored `TxnRoll.handle_roll_forward_mark` to reduce nesting depth
- Updated demo examples with working implementations for batch, expressions, and generation

- Added optional TLS for node connections (`:tls`, `:tls_opts` on `Aerospike.start_link/1`) via TCP connect plus `:ssl.connect/3` upgrade
- Documented how to run tests (unit, property, integration, coverage) in `README.md`
- Added ExCoveralls with 85% minimum coverage (`mix coveralls`, `mix test.coverage` for HTML report; integration tests included by default for accurate totals)
- Added unit tests for `AsmMsg.Field` and `AsmMsg.Operation` type accessors and decode error paths to meet the coverage threshold
- Raised minimum coverage to 85% with additional `PartitionMap`, `Policy`, and `Response` tests
- Added `Aerospike.Protocol.Message` for 8-byte protocol header encoding/decoding
- Added `Aerospike.Protocol.Info` for info command request/response encoding
- Added `Aerospike.Protocol.ResultCode` with complete integer-to-atom mapping for all Aerospike result codes
- Added `Aerospike.Protocol.AsmMsg` for AS_MSG encoding/decoding with field and operation sections
- Added `Aerospike.Protocol.AsmMsg.Field` for encoding/decoding wire protocol fields
- Added `Aerospike.Protocol.AsmMsg.Operation` for encoding/decoding wire protocol operations
- Added `Aerospike.Key` with RIPEMD-160 digest computation per Aerospike wire protocol
- Added `Aerospike.Record` struct for read responses with bins, generation, and TTL
- Added `Aerospike.Error` defexception with result code atoms for structured error handling
- Added `Aerospike.Protocol.AsmMsg.Value` for bin value encoding/decoding (integers, floats, strings, booleans, nil)
- Added `Aerospike.Protocol.Response` for AS_MSG response parsing (record, write, delete, exists)
- Added `Aerospike.CRUD` wire message builders (`build_put`, `build_get`, `build_delete`, `build_exists`, `build_touch`) for CRUD commands
- Added property-based tests for protocol invariants
- Added golden-file fixtures for protocol byte compatibility testing
- Added integration tests for full CRUD round-trip against Docker Aerospike
