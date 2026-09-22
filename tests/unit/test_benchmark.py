from benchmarks.benchmark_http import (
    percentile,
    summarize,
)


def test_percentile():
    values = [
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
    ]

    assert percentile(
        values,
        0.50,
    ) == 3.0


def test_empty_percentile():
    assert percentile([], 0.95) == 0.0


def test_summary():
    result = summarize(
        latencies=[
            0.010,
            0.020,
            0.030,
        ],
        successes=3,
        failures=0,
        duration=0.1,
    )

    assert result["requests"] == 3
    assert result["successes"] == 3
    assert result["failures"] == 0
    assert result["throughput_rps"] == 30.0
    assert result["latency_p50_ms"] == 20.0