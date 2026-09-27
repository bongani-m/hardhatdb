# Stress summary

Generated 2026-09-25T20:13:37-05:00.

Each target loaded 5000 accounts and 10000 notes, then ran 8 clients for 20s with 80% reads. Cluster writes go to the leader and reads go to the followers. MySQL flushes the redo log and the binlog on commit. The single node fsyncs each commit. The cluster also waits for a Raft quorum. TiDB reads and writes go through one SQL server to a three-node TiKV group. A commit waits for two Raft quorums.

CPU and memory are sampled about once a second while the client runs, including seed and warmup. 100% CPU is one core. A cluster figure is the sum of its nodes. Disk is the data directory after the run: `/data` on a persist node (Badger plus the Raft log) and `/var/lib/mysql` on MySQL. TiDB disk is the sum of the TiKV data directories.

| Target | ops/s | errors | p50 | p95 | p99 | seed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tidb | 3322.8 | 0 | 1.75ms | 6.78ms | 9.69ms | 0.86s |

| Target | CPU avg | CPU peak | mem avg | mem peak | disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| tidb | 553.5% | 629.6% | 6.9 GiB | 7.0 GiB | 13.7 GiB |

## tidb

Writes `127.0.0.1:3346`. Reads `127.0.0.1:3346`.

| node | CPU avg | CPU peak | mem avg | mem peak | disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| pd | 44.5% | 51.2% | 79.9 MiB | 86.3 MiB | - |
| tikv1 | 48.4% | 54.6% | 2.1 GiB | 2.2 GiB | 4.6 GiB |
| tikv2 | 176.0% | 202.6% | 2.3 GiB | 2.3 GiB | 4.6 GiB |
| tikv3 | 48.6% | 55.5% | 2.1 GiB | 2.2 GiB | 4.6 GiB |
| tidb | 236.1% | 273.7% | 358.0 MiB | 398.2 MiB | - |

| op | ops | errors | ops/s | avg | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| point_read | 33588 | 0 | 1679.4 | 1.42ms | 1.35ms | 2.07ms | 2.85ms | 30.91ms |
| email_read | 12952 | 0 | 647.6 | 2.16ms | 2.05ms | 3.07ms | 4.15ms | 23.01ms |
| notes_read | 6645 | 0 | 332.2 | 2.65ms | 2.50ms | 3.75ms | 5.38ms | 31.55ms |
| insert_note | 6743 | 0 | 337.1 | 4.78ms | 4.41ms | 6.63ms | 10.63ms | 106.10ms |
| update_status | 3350 | 0 | 167.5 | 2.00ms | 1.81ms | 3.01ms | 5.03ms | 103.41ms |
| tx_note | 3178 | 0 | 158.9 | 8.68ms | 8.20ms | 11.62ms | 19.68ms | 70.18ms |

