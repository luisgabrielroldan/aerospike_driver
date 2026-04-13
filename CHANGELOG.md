# Changelog

## 0.3.0 (2026-04-13)


### Features

* add Aerospike.Ctx for nested CDT paths ([51ccf7c](https://github.com/luisgabrielroldan/aerospike_driver/commit/51ccf7c712735b57b524dd9a32714f0647a9d88b))
* add Aerospike.Op primitive operate builders ([919926b](https://github.com/luisgabrielroldan/aerospike_driver/commit/919926bb22909745559f6aaeb4e22cbd40dc6b59))
* add Aerospike.Op.Bit bitwise CDT operations ([d7b09e9](https://github.com/luisgabrielroldan/aerospike_driver/commit/d7b09e922783ce13ec48624fcf869981e5f077f4))
* add Aerospike.Op.HLL HyperLogLog operations ([ad9622c](https://github.com/luisgabrielroldan/aerospike_driver/commit/ad9622c08972ad4b3ff10a35d04e26f76c9772fe))
* add Aerospike.Op.List CDT operations ([67acac8](https://github.com/luisgabrielroldan/aerospike_driver/commit/67acac842e8ea9e702b0b4c74df9f628be06ffbf))
* add Aerospike.Op.Map CDT operations ([1c553f3](https://github.com/luisgabrielroldan/aerospike_driver/commit/1c553f3a9face8cacfdf692ef26468280d233ce0))
* add Aerospike.put_payload/4 raw wire write ([b7247e8](https://github.com/luisgabrielroldan/aerospike_driver/commit/b7247e8fa67fed4cf15bc952f4a0b6df3b79a2a0))
* add batch operations, add/append/prepend commands, and streaming support ([b1d4271](https://github.com/luisgabrielroldan/aerospike_driver/commit/b1d42718054e52e713927ba63fb8412dd13a6c0b))
* add enterprise TLS/mTLS demo stack and fix QueryAggregate SC-mode failure ([7637155](https://github.com/luisgabrielroldan/aerospike_driver/commit/763715579536bbbe4755dd1951b77130acdcb782))
* add minimal MessagePack codec for CDT payloads ([54c818a](https://github.com/luisgabrielroldan/aerospike_driver/commit/54c818a25d622179137e259e7311f3528e8ccdf8))
* add operate/4 with policy and wire flag handling ([6101730](https://github.com/luisgabrielroldan/aerospike_driver/commit/61017302aba89f57d76e7e19dd8feb20c4544919))
* add Protocol.Exp expression wire encoder (Phase 9, Task 1) ([c355dd2](https://github.com/luisgabrielroldan/aerospike_driver/commit/c355dd2ae57ea2020af1e40f01b76c24aefdd764))
* add release please ([4a373d7](https://github.com/luisgabrielroldan/aerospike_driver/commit/4a373d7b6ef852f72830580ae0419e2d2d5d8ac3))
* **admin:** add udf inventory listing ([7ab785a](https://github.com/luisgabrielroldan/aerospike_driver/commit/7ab785ac92c00c22f1932aa854e9fa7922a41ea4))
* **admin:** add XDR filter management ([6152583](https://github.com/luisgabrielroldan/aerospike_driver/commit/615258317eb625e4e2af3cf211cade502e3c186e))
* **batch:** add phase 4 convenience and node-targeted APIs ([3d957da](https://github.com/luisgabrielroldan/aerospike_driver/commit/3d957daf293fa8b86bc3014eece89b9b59192e29))
* **bench:** add multi-node stream concurrency benchmark suite ([82b9ca7](https://github.com/luisgabrielroldan/aerospike_driver/commit/82b9ca73045c9774504cf27752d6babaefe62b51))
* **bench:** simplify benchmark workflow and developer docs ([b8eb2cf](https://github.com/luisgabrielroldan/aerospike_driver/commit/b8eb2cfe3fc3bb3e23326ff8c1eb18cc365f13d2))
* **circuit-breaker:** add policy-driven failure gating across operations ([908d247](https://github.com/luisgabrielroldan/aerospike_driver/commit/908d247a1a99a3bbdb85a248be26d8aeed12d10b))
* **examples:** add AeroMarketLive Phoenix LiveView demo ([f91c21c](https://github.com/luisgabrielroldan/aerospike_driver/commit/f91c21ca66bcb0b35f2de65bcb908090599ba9cd))
* fix test infrastructure for full suite (cluster + enterprise) ([6ccb5a8](https://github.com/luisgabrielroldan/aerospike_driver/commit/6ccb5a88f0f9586396cbdc5dd888247bf17f6960))
* **geo:** add typed geospatial API and docs updates ([489a02e](https://github.com/luisgabrielroldan/aerospike_driver/commit/489a02e22fbb0cd40cdf9de2996841aca7b89c7d))
* **index:** define advanced secondary index api ([fabfcdf](https://github.com/luisgabrielroldan/aerospike_driver/commit/fabfcdf38be32a87e02cf658a1af4a1a51524951))
* **index:** support advanced index create commands ([85fdece](https://github.com/luisgabrielroldan/aerospike_driver/commit/85fdece1867425eb0f02a6fcbf8dc8f787d5bc15))
* **op:** support float delta in add/2 and expand Map CDT docs ([c867667](https://github.com/luisgabrielroldan/aerospike_driver/commit/c8676678efd72313a59720e20e2b781cd84daef1))
* Phase 10 — GeoJSON encoding, SC-query streaming, transaction fixes, guides ([f74d8c3](https://github.com/luisgabrielroldan/aerospike_driver/commit/f74d8c3b7d5f79c34e32202a0ea349de4371832d))
* Phase 10 — transactions, admin, UDF, secondary indexes (Tasks 1–13) ([e175fa4](https://github.com/luisgabrielroldan/aerospike_driver/commit/e175fa4e5755abf325a2d53d4a6ff4e29c4deb62))
* Phase 8 — Scan, Query, streaming, and pagination ([789e7ad](https://github.com/luisgabrielroldan/aerospike_driver/commit/789e7ad9f5d1122c8c260d4692bbd99002ec5593))
* Phase 9 — Expressions (Aerospike.Exp, Op.Exp, CRUD filter) ([2abb38c](https://github.com/luisgabrielroldan/aerospike_driver/commit/2abb38c61cd1d3e5daf3b6c7f2ce0c0e62bf63e9))
* **protocol:** add shared CDT MessagePack operation encoder ([b39260d](https://github.com/luisgabrielroldan/aerospike_driver/commit/b39260d147058eb250414c4e0f985f51ceb7a7f7))
* **protocol:** decode map/list bins and particle-wrap nested MessagePack ([03e6435](https://github.com/luisgabrielroldan/aerospike_driver/commit/03e6435c264d8124671aa0226b7157eb9c6fbe46))
* **query:** add aggregate query streaming ([ab5b0ce](https://github.com/luisgabrielroldan/aerospike_driver/commit/ab5b0ceb72b801cd3b238258f177100b8a020240))
* **query:** add background execute task support ([8d57a85](https://github.com/luisgabrielroldan/aerospike_driver/commit/8d57a85a9ecf9e5aad4e9044577e1a2617117d28))
* **query:** add node-targeted background query APIs ([81d0696](https://github.com/luisgabrielroldan/aerospike_driver/commit/81d0696ef1ddeb41d24af817df52f8fceceed8f2))
* **query:** define phase 2 query API surface ([796243f](https://github.com/luisgabrielroldan/aerospike_driver/commit/796243f8b8fd20fae2baef98eb36da3afba1c8d9))
* **query:** support advanced secondary index targeting ([0fdaa69](https://github.com/luisgabrielroldan/aerospike_driver/commit/0fdaa69d65a458034e6da2c856d45d6403c0b9a6))
* **repo:** add Aerospike.Repo and migrate demo to repo-first APIs ([f2f6342](https://github.com/luisgabrielroldan/aerospike_driver/commit/f2f6342b91689ba7b172b3c792162bcc2b9a8c01))
* **runtime:** add client metrics and stats ([f484077](https://github.com/luisgabrielroldan/aerospike_driver/commit/f4840775ca7475df0dd967db969907fcbd103c6c))
* **runtime:** add connection pool warmup ([5f63911](https://github.com/luisgabrielroldan/aerospike_driver/commit/5f639111cdbe16368c0f30ebb7c279df9add5ea4))
* **security:** add security administration support ([31dd709](https://github.com/luisgabrielroldan/aerospike_driver/commit/31dd70976ed86ea8c5ab540c76b88b85c3b35e68))
* **stream:** enable concurrent multi-node stream fan-out ([03bda70](https://github.com/luisgabrielroldan/aerospike_driver/commit/03bda7069fcb760b159e8d765bbfdd7c8baba275))
* support tuple key inputs across public APIs ([ba78c3c](https://github.com/luisgabrielroldan/aerospike_driver/commit/ba78c3c00d97b2ae8a4738b3baa371e8619d1223))


### Bug Fixes

* **admin:** handle missing connection tables for udf inventory ([d813296](https://github.com/luisgabrielroldan/aerospike_driver/commit/d8132961fb9f620ce1438f10fd358fcd87aa4509))
* **cdt:** normalize ordered sentinels and complete return helpers ([8195ed5](https://github.com/luisgabrielroldan/aerospike_driver/commit/8195ed51ec49f42c4757171c9c6a69bab63d4015))
* **ci:** align test bootstrap with make targets ([40d1a79](https://github.com/luisgabrielroldan/aerospike_driver/commit/40d1a799f2447d100d0b6746af4a0d236e5da365))
* **ci:** increase truncate test timeout for slow CI runners ([1a3bc09](https://github.com/luisgabrielroldan/aerospike_driver/commit/1a3bc095becfc35c0657577d216a5c7763ea7229))
* **ci:** resolve enterprise roster race condition on slow CI runners ([a53978a](https://github.com/luisgabrielroldan/aerospike_driver/commit/a53978a4a1da5a532463fbda01bcaad7f5906175))
* **ci:** stabilize compose-backed test jobs ([7e4ae2e](https://github.com/luisgabrielroldan/aerospike_driver/commit/7e4ae2e3f176edd3cb74b521fd70c1520cf7ee31))
* **cluster:** iterate tend conns in tend_refresh_peers until one advances ([0586ccb](https://github.com/luisgabrielroldan/aerospike_driver/commit/0586ccbeee009f2579cf2485e44aecc92e9420ab))
* **cluster:** refresh partitions for peers discovered after bootstrap ([cebc05d](https://github.com/luisgabrielroldan/aerospike_driver/commit/cebc05dd51837555b57c13204bf85fc6d9b26255))
* **cluster:** return :empty_partition_map when fanout yields zero rows ([a50474d](https://github.com/luisgabrielroldan/aerospike_driver/commit/a50474dfbb529f3e8c3c273926bc2cadd343b987))
* **cluster:** track partition_generation per node ([6612ba3](https://github.com/luisgabrielroldan/aerospike_driver/commit/6612ba38ab408870278f4a48f1f52689efb926a3))
* **cluster:** wait for node pool replacement before reusing rotated credentials ([a686b42](https://github.com/luisgabrielroldan/aerospike_driver/commit/a686b42970639ef2bb81124a111cb97b1611121c))
* **crud:** remove unreachable overload error match ([83150ed](https://github.com/luisgabrielroldan/aerospike_driver/commit/83150edb36d4ffb8961f8aabfd9473557e65c848))
* **indexes:** align lifecycle command compatibility ([971f5c4](https://github.com/luisgabrielroldan/aerospike_driver/commit/971f5c4a95bd71df7fc1e9a780ddc04623af0ad0))
* **ops:** gate deps on cluster-stable instead of asinfo status ([a96dbd0](https://github.com/luisgabrielroldan/aerospike_driver/commit/a96dbd08cf5b9fc7f9381e0af7a582d2c5f0da4c))
* **protocol:** encode CDT {:bytes} as particle BLOB in MessagePack ([2cd3f4c](https://github.com/luisgabrielroldan/aerospike_driver/commit/2cd3f4ca882aeabe24e08d6f9758eb5d5bcb5a55))
* **protocol:** reduce batch response parse allocations with size-based skipping ([65acf76](https://github.com/luisgabrielroldan/aerospike_driver/commit/65acf769d779537940f58d1be740f061ebcf6cf1))
* **protocol:** switch asm message encode pipeline to iodata ([c02f37c](https://github.com/luisgabrielroldan/aerospike_driver/commit/c02f37cefde99014d66589081a4b4781636fad20))
* **repo:** remove constant conn warning and stabilize concurrent stream test ([cd45606](https://github.com/luisgabrielroldan/aerospike_driver/commit/cd45606cd3d6ef0e1f127a442336c4aaff0cddb5))
* resolve three pending issues and expand test coverage ([68dfe13](https://github.com/luisgabrielroldan/aerospike_driver/commit/68dfe1305997e36390b5facaa1cb0ac90d169266))
* **scan:** paginate all/2 correctly with cursor iteration and max_records cap ([1477349](https://github.com/luisgabrielroldan/aerospike_driver/commit/147734982f3903fdd996d2644cabb8947f9f7ef7))
* **security:** harden admin password rotation flows ([a686b42](https://github.com/luisgabrielroldan/aerospike_driver/commit/a686b42970639ef2bb81124a111cb97b1611121c))
* **test:** add compose-managed security node ([dcdad33](https://github.com/luisgabrielroldan/aerospike_driver/commit/dcdad33e13ef930b95a9309472892f3585586754))
* **test:** increase mocked admin client timeouts ([b71ad95](https://github.com/luisgabrielroldan/aerospike_driver/commit/b71ad959dd91133c27665df3003cab085ea7dc46))
* **test:** widen mock admin socket timeout ([71c1add](https://github.com/luisgabrielroldan/aerospike_driver/commit/71c1add162db95c59ac39b06b2c7a4f394e3bb19))


### Performance Improvements

* **batch-encoder:** use iodata accumulation instead of binary concat ([2bf3325](https://github.com/luisgabrielroldan/aerospike_driver/commit/2bf3325197a8e1b889d3ab6fb69bb7bf8fa16e80))


### Miscellaneous Chores

* fix release version ([37fd37f](https://github.com/luisgabrielroldan/aerospike_driver/commit/37fd37fd25a11dffbc9dbeddb376f686cea1f380))
* fix release version ([e68e320](https://github.com/luisgabrielroldan/aerospike_driver/commit/e68e320647c713dc687b6569e0c4eab21080f6f9))
* initial release ([1481512](https://github.com/luisgabrielroldan/aerospike_driver/commit/1481512731489efde1437fb2fc1dbd064f712553))

## [0.2.0](https://github.com/luisgabrielroldan/aerospike_driver/compare/v0.1.1...v0.2.0) (2026-04-09)


### Features

* **bench:** add multi-node stream concurrency benchmark suite ([d1e7367](https://github.com/luisgabrielroldan/aerospike_driver/commit/d1e736775fbea8ffca9740a7d2a76473b50e49c4))
* **bench:** simplify benchmark workflow and developer docs ([cb79e66](https://github.com/luisgabrielroldan/aerospike_driver/commit/cb79e660ad85afe672c89c4063d84f0fe0c1f909))
* **circuit-breaker:** add policy-driven failure gating across operations ([53a1288](https://github.com/luisgabrielroldan/aerospike_driver/commit/53a1288edfe0b1e1c9d2528d083a656687d71b6d))
* **geo:** add typed geospatial API and docs updates ([87bf2fb](https://github.com/luisgabrielroldan/aerospike_driver/commit/87bf2fb5d4bf5381fff9306ef32c1a405a1ed2ab))
* **stream:** enable concurrent multi-node stream fan-out ([20d1ca9](https://github.com/luisgabrielroldan/aerospike_driver/commit/20d1ca9f32a08cfc85bc5dd6e82d80944b186fc8))


### Bug Fixes

* **crud:** remove unreachable overload error match ([caf7232](https://github.com/luisgabrielroldan/aerospike_driver/commit/caf72327138ec7844e1fbab23b7cde086c18af5f))
* **protocol:** reduce batch response parse allocations with size-based skipping ([8408997](https://github.com/luisgabrielroldan/aerospike_driver/commit/8408997fcd69b99f35e0eef51fc614639c82849d))
* **protocol:** switch asm message encode pipeline to iodata ([258d8e6](https://github.com/luisgabrielroldan/aerospike_driver/commit/258d8e62471826146041529bbd8f748397679788))
* **repo:** remove constant conn warning and stabilize concurrent stream test ([418843d](https://github.com/luisgabrielroldan/aerospike_driver/commit/418843ddf5404aa2410b20c9e8a0b29bc07c64fe))


### Performance Improvements

* **batch-encoder:** use iodata accumulation instead of binary concat ([74d6c1d](https://github.com/luisgabrielroldan/aerospike_driver/commit/74d6c1d3ec0ff2abd597a99c9065b463a64c660c))

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
