# Chaos Tests

Docker is the preferred long-term target for chaos tests, but some hosted
development environments do not provide a Docker daemon. The current fallback is
a process-based smoke script that starts three local `quorumd` processes, runs
the replicated settlement demo, kills node 1, verifies a post-kill write through
the surviving endpoints, and prints the seed plus log directory.

```sh
test/chaos/process-settlement-smoke.sh 1
```

Run a sequential seed range:

```sh
test/chaos/process-settlement-seeds.sh 1 3
```

Run a repeated local failover measurement. This starts a fresh three-process cluster per run, kills node 1, polls surviving nodes with short RPC timeouts, and reports milliseconds from leader kill to the first successful write/read confirmation:

```sh
test/chaos/process-failover-bench.sh 5 1
```

Run the Docker Compose leader-kill workflow when Docker is available:

```sh
deploy/scripts/docker-settlement-chaos.sh 1
```

Future Docker/Compose scripts should add network partitions, latency, and packet
loss. Every run should print the seed so failures are reproducible.

