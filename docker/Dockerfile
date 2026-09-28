# Builds the persistent MySQL server. go.mod replaces go-mysql-server with the
# sibling checkout, passed in as an extra build context named "go-mysql-server".
FROM golang:1.26-bookworm AS build

RUN apt-get update \
	&& apt-get install -y --no-install-recommends g++ libicu-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY --from=go-mysql-server . /src/go-mysql-server
COPY . /src/hardhatdb

WORKDIR /src/hardhatdb/go
RUN go build -o /out/hardhatdb ./cmd/hardhatdb

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libicu72 \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /out/hardhatdb /usr/local/bin/hardhatdb
COPY LICENSE /usr/share/doc/hardhatdb/LICENSE
COPY NOTICE /usr/share/doc/hardhatdb/NOTICE
COPY licenses/MPL-2.0 /usr/share/doc/hardhatdb/MPL-2.0

WORKDIR /data
ENV HARDHATDB_DATA=/data/hardhatdb
EXPOSE 3306 7001
ENTRYPOINT ["hardhatdb", "sql-server"]
