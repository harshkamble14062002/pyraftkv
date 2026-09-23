# PyRaftKV local benchmark summary

> These are local development-machine measurements and are not representative of production cluster performance.

| Scenario | Requests | Successes | Failures | Throughput (req/s) | Mean (ms) | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| normal_put | 100 | 100 | 0 | 2267.47 | 4.174 | 3.998 | 6.33 | 6.731 |
| linearizable_get | 100 | 100 | 0 | 13294.23 | 0.426 | 0.007 | 4.832 | 5.507 |
| follower_unavailable | 100 | 100 | 0 | 2139.91 | 4.389 | 4.237 | 5.812 | 6.165 |
| mixed_workload | 100 | 100 | 0 | 2722.96 | 3.339 | 4.242 | 6.273 | 6.389 |
| leader_failover | 1 | 1 | 0 | 3528.34 | 0.283 | 0.283 | 0.283 | 0.283 |
| follower_catch_up | 1 | 1 | 0 | 1608.95 | 0.622 | 0.622 | 0.622 | 0.622 |
| snapshot_catch_up | 1 | 1 | 0 | 6255.12 | 0.16 | 0.16 | 0.16 | 0.16 |
