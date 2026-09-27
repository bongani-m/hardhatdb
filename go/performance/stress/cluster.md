# Stress summary

Generated 2026-09-25T20:17:01-05:00.

Each target loaded 5000 accounts and 10000 notes, then ran 8 clients for 20s with 80% reads. Cluster writes go to the leader and reads go to the followers. MySQL flushes the redo log and the binlog on commit. The single node fsyncs each commit. The cluster also waits for a Raft quorum. TiDB reads and writes go through one SQL server to a three-node TiKV group. A commit waits for two Raft quorums.

CPU and memory are sampled about once a second while the client runs, including seed and warmup. 100% CPU is one core. A cluster figure is the sum of its nodes. Disk is the data directory after the run: `/data` on a persist node (Badger plus the Raft log) and `/var/lib/mysql` on MySQL. TiDB disk is the sum of the TiKV data directories.

| Target | ops/s | errors | p50 | p95 | p99 | seed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| gms-cluster | 2038.3 | 0 | 1.09ms | 17.56ms | 25.41ms | 15.00s |

| Target | CPU avg | CPU peak | mem avg | mem peak | disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| gms-cluster | 136.1% | 204.1% | 421.8 MiB | 517.5 MiB | 6.6 GiB |

## gms-cluster

Writes `127.0.0.1:3326`. Reads `127.0.0.1:3327`, `127.0.0.1:3328`.

| node | CPU avg | CPU peak | mem avg | mem peak | disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| n1 | 52.2% | 77.9% | 146.8 MiB | 188.1 MiB | 2.2 GiB |
| n2 | 41.7% | 71.1% | 138.4 MiB | 165.8 MiB | 2.2 GiB |
| n3 | 42.1% | 75.8% | 136.5 MiB | 163.6 MiB | 2.2 GiB |

| op | ops | errors | ops/s | avg | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| point_read | 20584 | 0 | 1029.2 | 1.16ms | 939µs | 2.25ms | 5.13ms | 49.31ms |
| email_read | 7997 | 0 | 399.9 | 1.19ms | 965µs | 2.27ms | 5.36ms | 87.76ms |
| notes_read | 4176 | 0 | 208.8 | 1.30ms | 1.08ms | 2.44ms | 5.22ms | 49.29ms |
| insert_note | 3938 | 0 | 196.9 | 16.64ms | 15.07ms | 26.08ms | 40.34ms | 176.66ms |
| update_status | 2091 | 0 | 104.5 | 9.20ms | 8.14ms | 16.11ms | 23.71ms | 97.05ms |
| tx_note | 1980 | 0 | 99.0 | 18.32ms | 16.68ms | 28.50ms | 43.11ms | 175.76ms |

