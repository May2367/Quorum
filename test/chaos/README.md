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

Run a repeated local restart-recovery measurement. This commits a durable key, stops all three processes, restarts them from the same data directories, and reports milliseconds until the recovered key is readable and a new write commits:

```sh
test/chaos/process-restart-bench.sh 5 1
```

Run the Docker Compose leader-kill workflow when Docker is available:

```sh
deploy/scripts/docker-settlement-chaos.sh 1
```

Run the Docker Compose `tc netem` workflow from a Docker-capable VM. The restricted LoomAI workspace cannot run Docker directly, so use the LoomAI CLI to allocate a separate VM slice for this validation:

```sh
deploy/scripts/docker-netem-chaos.sh 1
```

The netem workflow uses a debug runtime image with `iproute2` and `NET_ADMIN`, injects latency/loss on `quorum1`, runs Porcupine-checked KV histories through `quorum2`/`quorum3`, clears the qdisc, and verifies a post-heal write. Every run prints the seed so failures are reproducible.

