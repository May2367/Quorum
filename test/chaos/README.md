# Chaos Tests

Chaos coverage has both process-based and Docker-based paths. Some hosted
development environments do not provide a Docker daemon, so the process scripts
run three local `quorumd` processes, exercise settlement/failover/restart
scenarios, and print seeds plus log directories for replay.

Docker workflows are intended for Docker-capable hosts. The Docker netem workflow
was validated on a separate LoomAI/FABRIC VM, not inside the restricted workspace.

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

Run the Docker Compose `tc netem` workflow from a Docker-capable VM. The restricted LoomAI workspace cannot run Docker directly. Published validation was performed on a separate LoomAI/FABRIC VM for seeds 1-3:

```sh
deploy/scripts/docker-netem-chaos.sh 1
```

The netem workflow uses a debug runtime image with `iproute2` and `NET_ADMIN`, injects 250 ms delay and 15% packet loss on `quorum1`, runs Porcupine-checked KV histories through all three endpoints, clears the qdisc, and verifies a post-heal write. Every run prints the seed so failures are reproducible.

