#!/usr/bin/env bash
# Run the same workload against one persist node, the three-node cluster, MySQL, or TiDB.
#
# From this repo:
#
#   go/performance/stress/run.sh single
#   go/performance/stress/run.sh cluster
#   go/performance/stress/run.sh mysql
#   go/performance/stress/run.sh tidb
#   go/performance/stress/run.sh ranged
#   go/performance/stress/run.sh compare
#   go/performance/stress/run.sh failover
#
# Flags after -- are passed to the stress client:
#
#   go/performance/stress/run.sh compare -- -duration 60s -concurrency 32 -seed 20000
#
# compare runs the targets one after another so they do not share the CPU.
# Every run writes go/performance/stress/summary.md.
# KEEP=1 leaves the containers up after the run.
# Wipe stored data with:
#
#   docker compose -f go/performance/stress/compose.yaml -p gms-stress down -v
#
# Ports: single 3316, cluster 3326 3327 3328, MySQL 3336, TiDB 3346, ranged 3376-3381.
# The cluster client writes to whichever node is the Raft leader.
# User root, password stress, database stress.
# Every target serves TLS. The script creates stress/certs on first use.
# TiDB setup uses plaintext on the Docker network, then the client connects with TLS.

set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$dir/../../.." && pwd)
root=$(cd "$repo/go" && pwd)
compose=(docker compose -f "$dir/compose.yaml" -p gms-stress)
summary=""
results=$dir/results
json_files=()
tls_ca=$dir/certs/ca.crt
mkdir -p "$results"

# caching_sha2_password is refused without TLS, so every target uses this cert.
# The CA also authenticates Raft between the cluster nodes.
# An existing certificate without the ranged addresses cannot authenticate Raft.
if [[ -f "$dir/certs/server.crt" ]] && ! openssl x509 -in "$dir/certs/server.crt" -noout -ext subjectAltName 2>/dev/null | grep -q '10.119.0.2'; then
	rm -f "$dir/certs/server.crt" "$dir/certs/server.key" "$dir/certs/server.csr"
fi
if [[ ! -f "$tls_ca" || ! -f "$dir/certs/server.crt" || ! -f "$dir/certs/server.key" ]]; then
	mkdir -p "$dir/certs"
	openssl req -x509 -newkey rsa:2048 -nodes \
		-keyout "$dir/certs/ca.key" \
		-out "$tls_ca" \
		-days 365 \
		-subj "/CN=gms-stress-ca" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign"
	openssl req -newkey rsa:2048 -nodes \
		-keyout "$dir/certs/server.key" \
		-out "$dir/certs/server.csr" \
		-subj "/CN=gms-stress" \
		-addext "subjectAltName=DNS:localhost,DNS:r1,DNS:r2,DNS:r3,DNS:r4,DNS:r5,DNS:r6,IP:127.0.0.1,IP:10.117.0.2,IP:10.117.0.3,IP:10.117.0.4,IP:10.119.0.2,IP:10.119.0.3,IP:10.119.0.4,IP:10.119.0.5,IP:10.119.0.6,IP:10.119.0.7" \
		-addext "extendedKeyUsage=serverAuth,clientAuth"
	openssl x509 -req -in "$dir/certs/server.csr" \
		-CA "$tls_ca" -CAkey "$dir/certs/ca.key" -CAcreateserial \
		-out "$dir/certs/server.crt" -days 365 \
		-copy_extensions copy
	chmod 644 "$dir/certs/server.key" "$dir/certs/server.crt" "$tls_ca"
fi

target=${1:-}
shift || true
if [[ "${1:-}" == "--" ]]; then
	shift
fi
extra=("$@")

usage() {
	echo "usage: $0 single|cluster|mysql|tidb|ranged|compare|failover [-- stress flags]" >&2
	exit 2
}

case "$target" in
single | cluster | mysql | tidb | ranged | compare | failover) ;;
*) usage ;;
esac

profile=""
sampler_pid=""
cleanup() {
	status=$?
	stop_sampler
	if [[ $status -ne 0 && -n "$profile" ]]; then
		"${compose[@]}" --profile "$profile" logs --tail 80 || true
	fi
	if [[ -n "$profile" && "${KEEP:-}" != 1 ]]; then
		"${compose[@]}" --profile "$profile" down
	fi
}
trap cleanup EXIT

build_image() {
	"${compose[@]}" --profile single build
}

up() {
	profile=$1
	case "$profile" in
	mysql | tidb)
		"${compose[@]}" --profile "$profile" up -d
		;;
	*)
		build_image
		"${compose[@]}" --profile "$profile" up -d
		;;
	esac
	if [[ "$profile" == tidb ]]; then
		prepare_tidb
	fi
}

# TiDB answers a MySQL ping before TiKV can serve SQL. Retry DDL, then set the
# password the stress client uses. A reused volume already has that password.
prepare_tidb() {
	local network
	local deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		if docker inspect gms-stress-tidb-1 >/dev/null 2>&1; then
			break
		fi
		sleep 1
	done
	if ! docker inspect gms-stress-tidb-1 >/dev/null 2>&1; then
		echo "tidb container did not start" >&2
		exit 1
	fi
	network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' gms-stress-tidb-1 | awk '{print $1}')
	if [[ -z "$network" ]]; then
		echo "tidb container has no network" >&2
		exit 1
	fi
	docker run --rm --network "$network" --entrypoint bash mysql:8.4 -c '
set -euo pipefail
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
	if mysql --protocol=TCP -h tidb -P 4000 -uroot --ssl-mode=DISABLED --connect-timeout=3 \
		-e "CREATE DATABASE IF NOT EXISTS stress" >/dev/null 2>&1; then
		mysql --protocol=TCP -h tidb -P 4000 -uroot --ssl-mode=DISABLED --connect-timeout=3 \
			-e "ALTER USER '\''root'\''@'\''%'\'' IDENTIFIED BY '\''stress'\''"
		exit 0
	fi
	if mysql --protocol=TCP -h tidb -P 4000 -uroot -pstress --ssl-mode=DISABLED --connect-timeout=3 \
		-e "CREATE DATABASE IF NOT EXISTS stress" >/dev/null 2>&1; then
		exit 0
	fi
	sleep 2
done
echo "tidb did not become ready" >&2
exit 1
'
}

run_client() {
	local label=$1
	shift
	(
		cd "$root"
		go run ./performance/stress -label "$label" "$@" ${extra[@]+"${extra[@]}"}
	)
}

containers_for() {
	case "$1" in
	single) echo gms-stress-single-1 ;;
	cluster | failover) echo gms-stress-n1-1 gms-stress-n2-1 gms-stress-n3-1 ;;
	ranged) echo gms-stress-r1-1 gms-stress-r2-1 gms-stress-r3-1 gms-stress-r4-1 gms-stress-r5-1 gms-stress-r6-1 ;;
	mysql) echo gms-stress-mysql-1 ;;
	tidb) echo gms-stress-pd-1 gms-stress-tikv1-1 gms-stress-tikv2-1 gms-stress-tikv3-1 gms-stress-tidb-1 ;;
	*) return 1 ;;
	esac
}

# Disk for TiDB is the TiKV data directories. PD and the SQL server hold no table data.
disk_containers_for() {
	case "$1" in
	tidb) echo gms-stress-tikv1-1 gms-stress-tikv2-1 gms-stress-tikv3-1 ;;
	*) containers_for "$1" ;;
	esac
}

disk_path_for() {
	case "$1" in
	mysql) echo /var/lib/mysql ;;
	*) echo /data ;;
	esac
}

start_sampler() {
	local name=$1
	local out=$results/$name.stats
	: >"$out"
	local -a cs
	read -r -a cs <<<"$(containers_for "$name")"
	(
		while true; do
			docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "${cs[@]}" >>"$out" 2>/dev/null || true
			sleep 1
		done
	) &
	sampler_pid=$!
}

stop_sampler() {
	if [[ -n "$sampler_pid" ]]; then
		kill "$sampler_pid" 2>/dev/null || true
		wait "$sampler_pid" 2>/dev/null || true
		sampler_pid=""
	fi
}

# du of the data directory on each container. One line per container: name bytes.
write_disk() {
	local name=$1
	local path
	path=$(disk_path_for "$name")
	local -a cs
	read -r -a cs <<<"$(disk_containers_for "$name")"
	local out=$results/$name.disk
	: >"$out"
	local c bytes
	for c in "${cs[@]}"; do
		bytes=$(docker exec "$c" du -sb "$path" 2>/dev/null | awk 'NR==1 {print $1}')
		if [[ -z "$bytes" ]]; then
			bytes=$(docker exec "$c" du -sk "$path" | awk 'NR==1 {print $1 * 1024}')
		fi
		if [[ -z "$bytes" ]]; then
			echo "could not measure disk usage of $c:$path" >&2
			exit 1
		fi
		echo "$c $bytes" >>"$out"
	done
}

run_target() {
	local name=$1
	up "$name"
	local log=/dev/stdout
	if [[ -n "$summary" ]]; then
		log=$summary
	fi
	local json=$results/$name.json
	start_sampler "$name"
	set +e
	case "$name" in
	single)
		run_client gms-single -write 127.0.0.1:3316 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	cluster)
		local leader_port read_csv
		leader_port=$(cluster_leader_port)
		read_csv=$(cluster_read_addrs "$leader_port")
		run_client gms-cluster -write "127.0.0.1:$leader_port" -read "$read_csv" -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	mysql)
		run_client mysql -write 127.0.0.1:3336 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	tidb)
		run_client tidb -write 127.0.0.1:3346 -tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	ranged)
		local w0 w1 r0 r1
		w0=$(shard_leader_port 3376 3377 3378)
		w1=$(shard_leader_port 3379 3380 3381)
		r0=$(shard_read_addrs "$w0" 3376 3377 3378)
		r1=$(shard_read_addrs "$w1" 3379 3380 3381)
		run_client gms-ranged \
			-shard-write "127.0.0.1:$w0,127.0.0.1:$w1" \
			-shard-read "$r0,$r1" \
			-tls-ca "$tls_ca" -json "$json" | tee -a "$log"
		;;
	esac
	local client_status=${PIPESTATUS[0]}
	set -e
	stop_sampler
	if [[ $client_status -ne 0 ]]; then
		exit "$client_status"
	fi
	write_disk "$name"
	json_files+=("$json")
	if [[ "${KEEP:-}" != 1 ]]; then
		"${compose[@]}" --profile "$profile" down
		profile=""
	fi
}

write_summary() {
	(
		cd "$root"
		go run ./performance/stress -render "$dir/summary.md" "${json_files[@]}"
	)
}

# now_ms is unix time in milliseconds.
now_ms() {
	python3 -c 'import time; print(int(time.time()*1000))'
}

# cluster_leader_port is the host port of the current Raft leader.
# Writes through a follower prepare the statement locally and again on the
# leader, which made a 5000-account seed take about 10s instead of about 2s.
cluster_leader_port() {
	local row leader_raft leader_port
	row=$(raft_row 3326)
	leader_raft=$(printf '%s\n' "$row" | awk -F'\t' 'NR==1 {print $2}')
	read -r _ leader_port <<<"$(node_for "$leader_raft")"
	if [[ -z "$leader_port" ]]; then
		echo "could not map leader $leader_raft" >&2
		return 1
	fi
	echo "$leader_port"
}

# cluster_read_addrs is the comma-separated list of nodes that are not port.
cluster_read_addrs() {
	local leader_port=$1
	local port addrs=""
	for port in 3326 3327 3328; do
		if [[ "$port" == "$leader_port" ]]; then
			continue
		fi
		if [[ -n "$addrs" ]]; then
			addrs+=","
		fi
		addrs+="127.0.0.1:$port"
	done
	echo "$addrs"
}

# raft_row prints one SHOW RAFT STATUS line: role, leader, commit, applied, lag.
raft_row() {
	local port=$1
	local wait=${2:-15s}
	(
		cd "$root"
		go run ./performance/stress -exec "SHOW RAFT STATUS" -write "127.0.0.1:$port" -password stress -tls-ca "$tls_ca" -ready-wait "$wait"
	)
}

# shard_leader_port prints the host MySQL port of the leader for one shard.
# The first argument that answers is enough; every node in the shard shares the group.
shard_leader_port() {
	local port row leader_raft leader_port
	for port in "$@"; do
		if row=$(raft_row "$port" 60s); then
			leader_raft=$(printf '%s\n' "$row" | awk -F'\t' 'NR==1 {print $2}')
			read -r _ leader_port <<<"$(node_for "$leader_raft")"
			if [[ -n "$leader_port" ]]; then
				echo "$leader_port"
				return 0
			fi
		fi
	done
	echo "could not find a shard leader among $*" >&2
	return 1
}

# shard_read_addrs joins the shard ports other than the leader with '|'.
shard_read_addrs() {
	local leader_port=$1
	shift
	local port addrs=""
	for port in "$@"; do
		if [[ "$port" == "$leader_port" ]]; then
			continue
		fi
		if [[ -n "$addrs" ]]; then
			addrs+="|"
		fi
		addrs+="127.0.0.1:$port"
	done
	echo "$addrs"
}

# stress_exec runs one statement against a published port.
stress_exec() {
	local port=$1
	local query=$2
	(
		cd "$root"
		go run ./performance/stress -exec "$query" -write "127.0.0.1:$port" -password stress -tls-ca "$tls_ca" -ready-wait 15s
	)
}

# node_for maps a Raft advertise address to the container and the host MySQL port.
node_for() {
	case "$1" in
	10.117.0.2:7001) echo gms-stress-n1-1 3326 ;;
	10.117.0.3:7001) echo gms-stress-n2-1 3327 ;;
	10.117.0.4:7001) echo gms-stress-n3-1 3328 ;;
	10.119.0.2:7101) echo gms-stress-r1-1 3376 ;;
	10.119.0.3:7101) echo gms-stress-r2-1 3377 ;;
	10.119.0.4:7101) echo gms-stress-r3-1 3378 ;;
	10.119.0.5:7101) echo gms-stress-r4-1 3379 ;;
	10.119.0.6:7101) echo gms-stress-r5-1 3380 ;;
	10.119.0.7:7101) echo gms-stress-r6-1 3381 ;;
	*) return 1 ;;
	esac
}

run_failover() {
	up cluster
	local row leader_raft
	row=$(raft_row 3326)
	leader_raft=$(printf '%s\n' "$row" | awk -F'\t' 'NR==1 {print $2}')
	local leader_container leader_port
	read -r leader_container leader_port <<<"$(node_for "$leader_raft")"
	if [[ -z "$leader_container" ]]; then
		echo "could not map leader $leader_raft" >&2
		exit 1
	fi
	local write_port read_port
	for port in 3326 3327 3328; do
		if [[ "$port" == "$leader_port" ]]; then
			continue
		fi
		if [[ -z "$write_port" ]]; then
			write_port=$port
			continue
		fi
		read_port=$port
	done

	local phase=$results/failover.phase
	local mark=$results/failover.mark
	local json=$results/failover.json
	rm -f "$phase" "$mark"
	local log=/dev/stdout
	if [[ -n "$summary" ]]; then
		log=$summary
	fi
	start_sampler failover
	set +e
	run_client gms-failover \
		-write "127.0.0.1:$write_port" \
		-read "127.0.0.1:$read_port" \
		-tls-ca "$tls_ca" \
		-duration 60s \
		-phase-file "$phase" \
		-failover-mark "$mark" \
		-json "$json" > >(tee -a "$log") &
	local client_pid=$!
	local deadline=$((SECONDS + 180))
	while [[ ! -f "$phase" ]]; do
		if ! kill -0 "$client_pid" 2>/dev/null; then
			wait "$client_pid"
			echo "stress client exited before the measured run" >&2
			set -e
			stop_sampler
			exit 1
		fi
		if (( SECONDS > deadline )); then
			kill "$client_pid" 2>/dev/null || true
			echo "timed out waiting for the measured run" >&2
			set -e
			stop_sampler
			exit 1
		fi
		sleep 0.2
	done
	sleep 2
	if ! stress_exec "$write_port" "INSERT INTO stress.accounts (email, name, status, created_at) VALUES ('failover-marker@example.com', 'failover', 1, NOW(6))"; then
		echo "marker insert failed" >&2
		kill "$client_pid" 2>/dev/null || true
		set -e
		stop_sampler
		exit 1
	fi
	local kill_ms ready_ms
	kill_ms=$(now_ms)
	docker kill "$leader_container"
	deadline=$((SECONDS + 45))
	local new_leader=""
	while (( SECONDS < deadline )); do
		row=$(raft_row "$write_port" || true)
		new_leader=$(printf '%s\n' "$row" | awk -F'\t' 'NR==1 {print $2}')
		if [[ -n "$new_leader" && "$new_leader" != "$leader_raft" ]]; then
			break
		fi
		new_leader=""
		sleep 0.5
	done
	if [[ -z "$new_leader" ]]; then
		echo "no new leader after killing $leader_container" >&2
		kill "$client_pid" 2>/dev/null || true
		set -e
		stop_sampler
		exit 1
	fi
	docker start "$leader_container"
	# ready is the first successful write after the new leader is visible and
	# the killed node has been started. In-flight failures stay in the election
	# window; later failures are after the cluster is serving writes again.
	local probe_deadline=$((SECONDS + 30)) probe_ok=0
	while (( SECONDS < probe_deadline )); do
		if stress_exec "$write_port" "UPDATE stress.accounts SET name = 'failover' WHERE email = 'failover-marker@example.com'"; then
			probe_ok=1
			break
		fi
		sleep 0.3
	done
	if [[ $probe_ok -ne 1 ]]; then
		echo "writes did not recover after killing $leader_container" >&2
		kill "$client_pid" 2>/dev/null || true
		set -e
		stop_sampler
		exit 1
	fi
	ready_ms=$(now_ms)
	printf 'kill %s\nready %s\n' "$kill_ms" "$ready_ms" >"$mark"
	wait "$client_pid"
	local client_status=$?
	set -e
	stop_sampler
	if [[ $client_status -ne 0 ]]; then
		exit "$client_status"
	fi
	local port
	for port in 3326 3327 3328; do
		local seen=""
		local until=$((SECONDS + 60))
		while (( SECONDS < until )); do
			if seen=$(stress_exec "$port" "SELECT email FROM stress.accounts WHERE email = 'failover-marker@example.com'" 2>/dev/null); then
				if [[ "$seen" == *failover-marker@example.com* ]]; then
					break
				fi
			fi
			seen=""
			sleep 1
		done
		if [[ "$seen" != *failover-marker@example.com* ]]; then
			echo "marker row missing on port $port" >&2
			exit 1
		fi
	done
	write_disk failover
	json_files+=("$json")
}

if [[ "$target" == compare ]]; then
	summary=$dir/last-compare.txt
	: >"$summary"
	for name in single cluster mysql tidb ranged; do
		echo "======== $name ========" | tee -a "$summary"
		run_target "$name"
	done
	write_summary
	echo
	echo "comparison"
	grep '^SUMMARY ' "$summary" || true
	echo "full output: $summary"
	exit 0
fi

if [[ "$target" == failover ]]; then
	run_failover
	write_summary
	exit 0
fi

run_target "$target"
write_summary
