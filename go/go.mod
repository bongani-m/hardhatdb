module github.com/bongani-m/hardhatdb/go

go 1.26.2

require (
	github.com/cockroachdb/apd/v3 v3.2.3
	github.com/dgraph-io/badger/v4 v4.9.4
	github.com/dolthub/go-mysql-server v0.0.0
	github.com/dolthub/vitess v0.0.0-20260916192104-15c5c4158b37
	github.com/go-sql-driver/mysql v1.9.3
	github.com/google/uuid v1.6.0
	github.com/hashicorp/raft v1.7.0
	github.com/hashicorp/raft-boltdb v0.0.0-20231211162105-6c830fa4535e
	github.com/stretchr/testify v1.11.1
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dolthub/flatbuffers/v23 v23.3.3-dh.2 // indirect
	github.com/dolthub/go-icu-regex v0.0.0-20260610153742-72563bc7ca83 // indirect
	github.com/dolthub/jsonpath v0.0.2-0.20260807003725-336cd89c1c76 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gocraft/dbr/v2 v2.7.2 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/lestrrat-go/strftime v1.2.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.8.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.41.0 // indirect
	go.opentelemetry.io/otel/metric v1.41.0 // indirect
	go.opentelemetry.io/otel/trace v1.41.0 // indirect
	golang.org/x/mod v0.36.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/telemetry v0.0.0-20260508192327-42602be52be6 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/tools v0.45.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/src-d/go-errors.v1 v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/dolthub/go-mysql-server => ../../go-mysql-server
