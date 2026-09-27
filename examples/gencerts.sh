#!/usr/bin/env bash
# Write the local test CA and server certificate to examples/certs.
# Compose mounts that directory at /certs. The files are not committed.
# A later run leaves an existing certificate in place.

set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
out=$dir/certs

# n1–n3 are the single group. g1–g6 are the two groups in compose.sharded.yaml.
san="DNS:localhost,DNS:n1,DNS:n2,DNS:n3,DNS:g1,DNS:g2,DNS:g3,DNS:g4,DNS:g5,DNS:g6,IP:127.0.0.1,IP:10.116.0.2,IP:10.116.0.3,IP:10.116.0.4,IP:10.118.0.2,IP:10.118.0.3,IP:10.118.0.4,IP:10.118.0.5,IP:10.118.0.6,IP:10.118.0.7"

mkdir -p "$out"
if [[ ! -f "$out/ca.crt" || ! -f "$out/ca.key" ]]; then
	openssl req -x509 -newkey rsa:2048 -nodes \
		-keyout "$out/ca.key" \
		-out "$out/ca.crt" \
		-days 365 \
		-subj "/CN=hardhatdb-ca" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign"
fi
if [[ -f "$out/server.crt" ]] && openssl x509 -in "$out/server.crt" -noout -ext subjectAltName 2>/dev/null | grep -q '10.118.0.2'; then
	exit 0
fi
openssl req -newkey rsa:2048 -nodes \
	-keyout "$out/server.key" \
	-out "$out/server.csr" \
	-subj "/CN=hardhatdb" \
	-addext "subjectAltName=$san" \
	-addext "extendedKeyUsage=serverAuth,clientAuth"
openssl x509 -req -in "$out/server.csr" \
	-CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial \
	-out "$out/server.crt" -days 365 \
	-copy_extensions copy
