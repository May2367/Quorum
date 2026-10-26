# Chaos Tests

Docker is the preferred long-term target for chaos tests, but some hosted
development environments do not provide a Docker daemon. The current fallback is
a process-based smoke script that starts three local `quorumd` processes, runs
the replicated settlement demo, kills node 1, verifies a post-kill write through
the surviving endpoints, and prints the seed plus log directory.

```sh
test/chaos/process-settlement-smoke.sh 1
```

Future Docker/Compose scripts should add seeded leader kills, network
partitions, latency, and packet loss. Every run should print the seed so
failures are reproducible.

