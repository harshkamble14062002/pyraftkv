# Benchmarks

## Measurement policy

PyRaftKV benchmark results must come from executed commands and committed
machine-readable output. Results are reported as local development
measurements, not production or cloud capacity claims.

Every workload summary includes request count, successes, failures, total
duration, throughput, mean latency, and p50/p95/p99 latency.

## In-memory Raft suite

benchmarks/benchmark_raft.py runs a local three-node cluster over
InMemoryTransport. It covers:

- normal replicated PUT;
- leader-only linearizable GET;
- PUT with one follower unavailable;
- a deterministic mixed workload;
- leader failover;
- follower log catch-up;
- snapshot installation and retained-suffix catch-up.

Reproduce it with:

~~~bash
python -m benchmarks.benchmark_raft \
  --requests 1000 \
  --concurrency 10 \
  --recovery-entries 100 \
  --output benchmarks/results/local-raft-latest.json \
  --markdown benchmarks/results/local-raft-latest.md
~~~

The suite writes JSON for tooling and a Markdown summary for review.

## Committed Phase H result

Configuration:

- three in-process Raft nodes;
- in-memory transport;
- 100 requests per workload;
- concurrency 10;
- 50 entries in recovery scenarios;
- generated 2026-09-23.

> These are local development-machine measurements and are not representative
> of production cluster performance.

| Scenario | Requests | Successes | Failures | Throughput req/s | Mean ms | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| normal PUT | 100 | 100 | 0 | 2267.47 | 4.174 | 3.998 | 6.330 | 6.731 |
| linearizable GET | 100 | 100 | 0 | 13294.23 | 0.426 | 0.007 | 4.832 | 5.507 |
| follower unavailable | 100 | 100 | 0 | 2139.91 | 4.389 | 4.237 | 5.812 | 6.165 |
| mixed workload | 100 | 100 | 0 | 2722.96 | 3.339 | 4.242 | 6.273 | 6.389 |
| leader failover | 1 | 1 | 0 | 3528.34 | 0.283 | 0.283 | 0.283 | 0.283 |
| follower catch-up | 1 | 1 | 0 | 1608.95 | 0.622 | 0.622 | 0.622 | 0.622 |
| snapshot catch-up | 1 | 1 | 0 | 6255.12 | 0.160 | 0.160 | 0.160 | 0.160 |

The canonical artifacts are
[local-raft-phase-h.json](../benchmarks/results/local-raft-phase-h.json) and
[local-raft-phase-h.md](../benchmarks/results/local-raft-phase-h.md).

Recovery rows contain one measured operation and should be read primarily as
local duration observations, not stable throughput estimates.

## HTTP benchmark

benchmarks/benchmark_http.py sends concurrent GET or PUT requests to a supplied
leader URL:

~~~bash
python -m benchmarks.benchmark_http \
  --url http://127.0.0.1:8001 \
  --operation put \
  --requests 5000 \
  --concurrency 10 \
  --warmup 100 \
  --output benchmarks/results/http-put.json
~~~

Discover the current leader first. Requests sent to a follower are expected to
fail with HTTP 409 and would invalidate a leader-throughput comparison.

## Historical append-only persistence comparison

Two committed local HTTP artifacts record the effect of replacing full-log
rewrites on normal appends with journal appends:

| Artifact | Requests | Concurrency | Successes | Throughput req/s | Mean ms |
|---|---:|---:|---:|---:|---:|
| raft-put-c10-baseline.json | 5000 | 10 | 5000 | 14.74 | 678.533 |
| raft-put-c10-append-only.json | 5000 | 10 | 5000 | 107.86 | 92.614 |

The targets differed between runs because the elected leader differed. These
numbers are useful as local before/after engineering evidence, not a controlled
cross-machine comparison.

## Interpretation limits

The in-memory suite does not include HTTP serialization, container networking,
filesystem variability across hosts, TLS, or geographically distributed
latency. Small request counts make tail percentiles sensitive to local
scheduling.

For meaningful comparison:

- use the same code revision and Python version;
- record CPU, memory, filesystem, and container configuration;
- use the same leader discovery and warmup procedure;
- preserve raw JSON output;
- report failures rather than excluding them;
- run multiple trials and describe aggregation;
- avoid comparing in-memory results directly with HTTP results.

No benchmark in this repository is a service-level objective or capacity
guarantee.
