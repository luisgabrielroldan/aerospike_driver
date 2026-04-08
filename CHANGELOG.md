# Changelog

## 0.1.1 (2026-04-08)


### Features

* add Aerospike.Ctx for nested CDT paths ([51ccf7c](https://github.com/luisgabrielroldan/aerospike_driver/commit/51ccf7c712735b57b524dd9a32714f0647a9d88b))
* add Aerospike.Op primitive operate builders ([919926b](https://github.com/luisgabrielroldan/aerospike_driver/commit/919926bb22909745559f6aaeb4e22cbd40dc6b59))
* add Aerospike.Op.Bit bitwise CDT operations ([d7b09e9](https://github.com/luisgabrielroldan/aerospike_driver/commit/d7b09e922783ce13ec48624fcf869981e5f077f4))
* add Aerospike.Op.HLL HyperLogLog operations ([ad9622c](https://github.com/luisgabrielroldan/aerospike_driver/commit/ad9622c08972ad4b3ff10a35d04e26f76c9772fe))
* add Aerospike.Op.List CDT operations ([67acac8](https://github.com/luisgabrielroldan/aerospike_driver/commit/67acac842e8ea9e702b0b4c74df9f628be06ffbf))
* add Aerospike.Op.Map CDT operations ([1c553f3](https://github.com/luisgabrielroldan/aerospike_driver/commit/1c553f3a9face8cacfdf692ef26468280d233ce0))
* add batch operations, add/append/prepend commands, and streaming support ([b1d4271](https://github.com/luisgabrielroldan/aerospike_driver/commit/b1d42718054e52e713927ba63fb8412dd13a6c0b))
* add enterprise TLS/mTLS demo stack and fix QueryAggregate SC-mode failure ([7637155](https://github.com/luisgabrielroldan/aerospike_driver/commit/763715579536bbbe4755dd1951b77130acdcb782))
* add minimal MessagePack codec for CDT payloads ([54c818a](https://github.com/luisgabrielroldan/aerospike_driver/commit/54c818a25d622179137e259e7311f3528e8ccdf8))
* add operate/4 with policy and wire flag handling ([6101730](https://github.com/luisgabrielroldan/aerospike_driver/commit/61017302aba89f57d76e7e19dd8feb20c4544919))
* add Protocol.Exp expression wire encoder (Phase 9, Task 1) ([c355dd2](https://github.com/luisgabrielroldan/aerospike_driver/commit/c355dd2ae57ea2020af1e40f01b76c24aefdd764))
* add release please ([4a373d7](https://github.com/luisgabrielroldan/aerospike_driver/commit/4a373d7b6ef852f72830580ae0419e2d2d5d8ac3))
* fix test infrastructure for full suite (cluster + enterprise) ([6ccb5a8](https://github.com/luisgabrielroldan/aerospike_driver/commit/6ccb5a88f0f9586396cbdc5dd888247bf17f6960))
* Phase 10 — GeoJSON encoding, SC-query streaming, transaction fixes, guides ([f74d8c3](https://github.com/luisgabrielroldan/aerospike_driver/commit/f74d8c3b7d5f79c34e32202a0ea349de4371832d))
* Phase 10 — transactions, admin, UDF, secondary indexes (Tasks 1–13) ([e175fa4](https://github.com/luisgabrielroldan/aerospike_driver/commit/e175fa4e5755abf325a2d53d4a6ff4e29c4deb62))
* Phase 8 — Scan, Query, streaming, and pagination ([789e7ad](https://github.com/luisgabrielroldan/aerospike_driver/commit/789e7ad9f5d1122c8c260d4692bbd99002ec5593))
* Phase 9 — Expressions (Aerospike.Exp, Op.Exp, CRUD filter) ([2abb38c](https://github.com/luisgabrielroldan/aerospike_driver/commit/2abb38c61cd1d3e5daf3b6c7f2ce0c0e62bf63e9))
* **protocol:** add shared CDT MessagePack operation encoder ([b39260d](https://github.com/luisgabrielroldan/aerospike_driver/commit/b39260d147058eb250414c4e0f985f51ceb7a7f7))
* **protocol:** decode map/list bins and particle-wrap nested MessagePack ([03e6435](https://github.com/luisgabrielroldan/aerospike_driver/commit/03e6435c264d8124671aa0226b7157eb9c6fbe46))
* **repo:** add Aerospike.Repo and migrate demo to repo-first APIs ([f2f6342](https://github.com/luisgabrielroldan/aerospike_driver/commit/f2f6342b91689ba7b172b3c792162bcc2b9a8c01))
* support tuple key inputs across public APIs ([ba78c3c](https://github.com/luisgabrielroldan/aerospike_driver/commit/ba78c3c00d97b2ae8a4738b3baa371e8619d1223))


### Bug Fixes

* **ci:** increase truncate test timeout for slow CI runners ([1a3bc09](https://github.com/luisgabrielroldan/aerospike_driver/commit/1a3bc095becfc35c0657577d216a5c7763ea7229))
* **ci:** resolve enterprise roster race condition on slow CI runners ([a53978a](https://github.com/luisgabrielroldan/aerospike_driver/commit/a53978a4a1da5a532463fbda01bcaad7f5906175))
* **protocol:** encode CDT {:bytes} as particle BLOB in MessagePack ([2cd3f4c](https://github.com/luisgabrielroldan/aerospike_driver/commit/2cd3f4ca882aeabe24e08d6f9758eb5d5bcb5a55))
* resolve three pending issues and expand test coverage ([68dfe13](https://github.com/luisgabrielroldan/aerospike_driver/commit/68dfe1305997e36390b5facaa1cb0ac90d169266))
* **scan:** paginate all/2 correctly with cursor iteration and max_records cap ([1477349](https://github.com/luisgabrielroldan/aerospike_driver/commit/147734982f3903fdd996d2644cabb8947f9f7ef7))


### Miscellaneous Chores

* initial release ([1481512](https://github.com/luisgabrielroldan/aerospike_driver/commit/1481512731489efde1437fb2fc1dbd064f712553))
