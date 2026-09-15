# PyRaftKV Architecture

## Overview

PyRaftKV is a fault-tolerant distributed key-value store built
from scratch in Python.

The system will provide:

- PUT
- GET
- DELETE
- Write-Ahead Logging
- crash recovery
- Raft leader election
- replicated logs
- automatic failover
- sharding
- consistent hashing
- monitoring and metrics

---

## High-Level Architecture

```text
                Client
                   |
                   v
              API / Router
                   |
                   v
              Raft Leader
              /         \
             v           v
        Follower      Follower
             \           /
              \         /
                Raft Log
                   |
                   v
             State Machine
                   |
                   v
              KV Storage
                   |
                   v
                  WAL