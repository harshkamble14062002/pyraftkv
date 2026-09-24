import json

from benchmarks.benchmark_raft import (
    DISCLAIMER,
    SUMMARY_FIELDS,
    markdown_summary,
    run_suite,
    write_results,
)


def test_local_raft_suite_reports_required_scenarios():
    result = run_suite(
        requests=12,
        concurrency=3,
        recovery_entries=6,
    )

    assert result["disclaimer"] == DISCLAIMER
    assert set(result["scenarios"]) == {
        "normal_put",
        "linearizable_get",
        "follower_unavailable",
        "mixed_workload",
        "leader_failover",
        "follower_catch_up",
        "snapshot_catch_up",
    }

    for scenario in result["scenarios"].values():
        assert set(SUMMARY_FIELDS).issubset(
            scenario
        )
        assert scenario["failures"] == 0

    assert (
        result["scenarios"]["snapshot_catch_up"][
            "install_snapshot_calls"
        ]
        >= 1
    )


def test_benchmark_results_are_machine_readable(
    tmp_path,
):
    result = run_suite(
        requests=6,
        concurrency=2,
        recovery_entries=4,
    )
    output = tmp_path / "result.json"
    markdown = tmp_path / "result.md"

    write_results(
        result,
        output,
        markdown,
    )

    loaded = json.loads(
        output.read_text(encoding="utf-8")
    )

    assert loaded["format_version"] == 1
    assert loaded["scenarios"]["normal_put"][
        "requests"
    ] == 6
    assert DISCLAIMER in markdown.read_text(
        encoding="utf-8"
    )


def test_markdown_summary_contains_percentiles():
    result = run_suite(
        requests=3,
        concurrency=1,
        recovery_entries=2,
    )

    summary = markdown_summary(result)

    assert "Throughput (req/s)" in summary
    assert "p50" in summary
    assert "p95" in summary
    assert "p99" in summary
