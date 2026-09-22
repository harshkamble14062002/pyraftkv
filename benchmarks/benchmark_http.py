import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx


def percentile(
    values: list[float],
    percent: float,
) -> float:
    if not values:
        return 0.0

    ordered = sorted(values)

    index = (len(ordered) - 1) * percent
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)

    fraction = index - lower

    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def summarize(
    latencies: list[float],
    successes: int,
    failures: int,
    duration: float,
) -> dict[str, float | int]:
    latency_ms = [latency * 1000 for latency in latencies]

    throughput = successes / duration if duration > 0 else 0.0

    return {
        "requests": successes + failures,
        "successes": successes,
        "failures": failures,
        "duration_seconds": round(duration, 4),
        "throughput_rps": round(throughput, 2),
        "latency_mean_ms": round(
            statistics.mean(latency_ms),
            3,
        )
        if latency_ms
        else 0.0,
        "latency_p50_ms": round(
            percentile(latency_ms, 0.50),
            3,
        ),
        "latency_p95_ms": round(
            percentile(latency_ms, 0.95),
            3,
        ),
        "latency_p99_ms": round(
            percentile(latency_ms, 0.99),
            3,
        ),
    }


def run_request(
    client: httpx.Client,
    base_url: str,
    operation: str,
    request_id: int,
) -> tuple[bool, float, str]:
    key = f"benchmark-{request_id}"

    start = time.perf_counter()

    try:
        if operation == "put":
            response = client.put(
                f"{base_url}/kv/{key}",
                json={
                    "value": f"value-{request_id}",
                },
            )

        elif operation == "get":
            response = client.get(f"{base_url}/kv/benchmark-key")

        else:
            raise ValueError(f"Unsupported operation: {operation}")

        success = 200 <= response.status_code < 300
        outcome = str(response.status_code)

    except httpx.TimeoutException:
        success = False
        outcome = "timeout"

    except httpx.HTTPError:
        success = False
        outcome = "network_error"

    latency = time.perf_counter() - start

    return success, latency, outcome


def prepare(
    client: httpx.Client,
    base_url: str,
    operation: str,
    warmup: int,
) -> None:
    if operation == "get":
        response = client.put(
            f"{base_url}/kv/benchmark-key",
            json={
                "value": "benchmark-value",
            },
        )

        response.raise_for_status()

    for index in range(warmup):
        if operation == "get":
            client.get(f"{base_url}/kv/benchmark-key")

        else:
            client.put(
                f"{base_url}/kv/warmup-{index}",
                json={
                    "value": "warmup",
                },
            )


def benchmark(
    base_url: str,
    operation: str,
    requests: int,
    concurrency: int,
    warmup: int,
    timeout: float,
) -> dict[str, object]:
    base_url = base_url.rstrip("/")

    client = httpx.Client(
        timeout=timeout,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=concurrency,
        ),
    )

    prepare(
        client,
        base_url,
        operation,
        warmup,
    )

    latencies: list[float] = []

    successes = 0
    failures = 0
    outcomes: dict[str, int] = {}

    started = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(
                run_request,
                client,
                base_url,
                operation,
                request_id,
            )
            for request_id in range(requests)
        ]

        for future in as_completed(futures):
            success, latency, outcome = future.result()

            outcomes[outcome] = (
                outcomes.get(
                    outcome,
                    0,
                )
                + 1
            )

            latencies.append(latency)

            if success:
                successes += 1
            else:
                failures += 1

    duration = time.perf_counter() - started

    client.close()

    return {
        "target": base_url,
        "operation": operation,
        "concurrency": concurrency,
        "warmup_requests": warmup,
        "outcomes": outcomes,
        **summarize(
            latencies,
            successes,
            failures,
            duration,
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark PyRaftKV")

    parser.add_argument(
        "--url",
        required=True,
    )

    parser.add_argument(
        "--operation",
        choices=["get", "put"],
        required=True,
    )

    parser.add_argument(
        "--requests",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=50,
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
    )

    parser.add_argument(
        "--output",
        type=Path,
    )

    args = parser.parse_args()

    if args.requests <= 0:
        parser.error("--requests must be positive")

    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")

    result = benchmark(
        base_url=args.url,
        operation=args.operation,
        requests=args.requests,
        concurrency=args.concurrency,
        warmup=args.warmup,
        timeout=args.timeout,
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )

    if args.output is not None:
        args.output.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        args.output.write_text(
            json.dumps(
                result,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
