FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/examplewebapp .

FROM debian:bookworm-slim

COPY --from=build /out/examplewebapp /usr/local/bin/examplewebapp

EXPOSE 8080
ENV HTTP_ADDR=:8080
ENTRYPOINT ["examplewebapp"]
