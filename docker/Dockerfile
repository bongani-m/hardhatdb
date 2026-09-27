# Builds the persistent MySQL server. go.mod replaces go-mysql-server with the
# sibling checkout, passed in as an extra build context named "gms".
FROM golang:1.26-bookworm AS build

RUN apt-get update \
	&& apt-get install -y --no-install-recommends g++ libicu-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY --from=gms . /src/go-mysql-server
COPY . /src/persist

WORKDIR /src/persist/go
RUN go build -o /out/persist ./cmd/persist

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libicu72 \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /out/persist /usr/local/bin/persist

WORKDIR /data
ENV GMS_DATA=/data/gms
EXPOSE 3306 7001
ENTRYPOINT ["persist", "sql-server"]
