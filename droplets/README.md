# Three droplets on DigitalOcean

Run one Raft group on three droplets in the same region and the same VPC. Clients talk to MySQL on the private addresses. Raft stays on those private addresses too. The public interface does not listen for either port.

Raft and the write-forward port require mutual TLS. The Cloud Firewall keeps ports 7001 and 7002 inside the group. MySQL requires TLS too.

Writes succeed on the Raft leader. A follower forwards writes to whichever node is the leader, so clients can keep using any MySQL address. A read on that same connection waits until the write is applied locally. Other connections can still lag.

`compose.yaml` in the parent directory is the same address plan on a bridge network named `vpc`. Local testing, including the test certificate, is in [the persist README](../README.md#local-testing). On droplets, use the `docker run` commands below. Three containers cannot share host networking on one machine, because each one binds ports 3306, 7001, and 7002.

## Droplets

| Name | Role | VPC address in this example |
|------|------|-----------------------------|
| n1 | Bootstraps the group the first time it starts | 10.116.0.2 |
| n2 | Joins the group | 10.116.0.3 |
| n3 | Joins the group | 10.116.0.4 |

Use the VPC addresses from the droplet page. The values above are placeholders, and they are also the addresses Compose assigns.

Create the droplets in one region, attach them to one VPC, and tag each with `gms`. Attach a Volume to each droplet and mount it at `/data`. The Badger directory and the Raft log both live on that volume.

## Firewall

One Cloud Firewall, applied to tag `gms`. Inbound rules:

| Type | Port | Sources |
|------|------|---------|
| SSH | 22 | Your own IP |
| MySQL | 3306 | Tag `gms`, plus the tag or addresses of the app droplets |
| Raft | 7001 | Tag `gms` |
| Forward | 7002 | Tag `gms` |

Leave every other inbound port closed. Do not add a public address to 3306 or 7001.

App droplets in this VPC reach MySQL on the private addresses. A client on the internet needs a later change: a certificate name for the public address, and a 3306 rule limited to that client. Raft stays on the VPC rule either way.

## Certificate

Create a CA and one certificate whose names are the addresses clients and peers dial. The same certificate authenticates MySQL, Raft, and the write-forward port. Run this once, then copy `ca.crt`, `server.crt`, and `server.key` to `/data/certs` on each droplet.

```bash
mkdir -p /data/certs
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /data/certs/ca.key \
  -out /data/certs/ca.crt \
  -days 365 \
  -subj "/CN=gms-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes \
  -keyout /data/certs/server.key \
  -out /data/certs/server.csr \
  -subj "/CN=gms" \
  -addext "subjectAltName=IP:10.116.0.2,IP:10.116.0.3,IP:10.116.0.4" \
  -addext "extendedKeyUsage=serverAuth,clientAuth"
openssl x509 -req -in /data/certs/server.csr \
  -CA /data/certs/ca.crt -CAkey /data/certs/ca.key -CAcreateserial \
  -out /data/certs/server.crt -days 365 \
  -copy_extensions copy
```

## Node environment

`/data/gms.env` on n1:

```bash
GMS_NODE_ID=n1
GMS_RAFT_ADDR=10.116.0.2:7001
GMS_RAFT_ADVERTISE=10.116.0.2:7001
GMS_RAFT_PEERS=n1=10.116.0.2:7001,n2=10.116.0.3:7001,n3=10.116.0.4:7001
GMS_RAFT_BOOTSTRAP=1
GMS_SERVER_UUID=11111111-1111-1111-1111-111111111111
GMS_RAFT_DIR=/data/raft
GMS_DATA=/data/gms
GMS_MYSQL_HOST=10.116.0.2
GMS_BOOTSTRAP_PASSWORD=replace-me
GMS_SEED_EXAMPLE=1
GMS_TLS_CERT=/data/certs/server.crt
GMS_TLS_KEY=/data/certs/server.key
GMS_RAFT_TLS_CERT=/data/certs/server.crt
GMS_RAFT_TLS_KEY=/data/certs/server.key
GMS_RAFT_TLS_CA=/data/certs/ca.crt
```

n2 and n3 use the same peers, the same `GMS_SERVER_UUID`, and the same password and certificate paths. Change `GMS_NODE_ID`, `GMS_RAFT_ADDR`, `GMS_RAFT_ADVERTISE`, and `GMS_MYSQL_HOST` to that droplet's VPC address. Leave `GMS_RAFT_BOOTSTRAP` unset on n2 and n3.

After n1 has joined the other two nodes, remove `GMS_RAFT_BOOTSTRAP` from n1 before the next restart. A restart that still has Raft state ignores the flag, logs that, and joins as a follower. A new empty volume with bootstrap left on still creates a second group.

`SHOW RAFT STATUS` on any node reports the role, the leader address, and how far that node has applied. Add or remove a voter with `RAFT ADD VOTER '<id>' '<host:port>'` and `RAFT REMOVE SERVER '<id>'`. A follower forwards those two statements to the leader.

## Start

Build the image from this repo the same way Compose does, then on each droplet:

```bash
docker run -d --name gms --restart unless-stopped \
  --network host \
  --env-file /data/gms.env \
  -v /data:/data \
  gms-persist
```

`--network host` makes `GMS_RAFT_ADDR` and `GMS_MYSQL_HOST` bind the droplet's VPC address. Publishing `-p 7001:7001` would listen on the public interface as well.

Start n2 and n3 first, then n1. n1 is the node that bootstraps.

## Clients

From an app droplet in the VPC:

```bash
mysql --host=10.116.0.2 --port=3306 --user=root --password=replace-me \
  --ssl-mode=REQUIRED --ssl-ca=/data/certs/ca.crt \
  mydb --execute="SELECT name, email FROM mytable;"
```

A write to any node is forwarded to the current leader. Example apps take every address and try the next one when a connection breaks:

```bash
MYSQL_ADDRS=10.116.0.2:3306,10.116.0.3:3306,10.116.0.4:3306
```
