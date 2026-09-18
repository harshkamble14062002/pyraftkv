from unittest.mock import patch

import pytest

from pyraftkv.raft.timer import ElectionTimer


def test_invalid_min_timeout():
    with pytest.raises(ValueError):
        ElectionTimer(
            min_timeout=0,
            max_timeout=0.3,
        )


def test_invalid_timeout_range():
    with pytest.raises(ValueError):
        ElectionTimer(
            min_timeout=0.3,
            max_timeout=0.2,
        )


@patch("pyraftkv.raft.timer.time.monotonic")
@patch("pyraftkv.raft.timer.random.uniform")
def test_timer_uses_random_timeout(
    mock_uniform,
    mock_monotonic,
):
    mock_monotonic.return_value = 10.0
    mock_uniform.return_value = 0.2

    timer = ElectionTimer(
        min_timeout=0.15,
        max_timeout=0.3,
    )

    assert timer._deadline == 10.2

    mock_uniform.assert_called_once_with(
        0.15,
        0.3,
    )


@patch("pyraftkv.raft.timer.time.monotonic")
@patch("pyraftkv.raft.timer.random.uniform")
def test_timer_expires(
    mock_uniform,
    mock_monotonic,
):
    mock_uniform.return_value = 0.2
    mock_monotonic.return_value = 10.0

    timer = ElectionTimer()

    mock_monotonic.return_value = 10.21

    assert timer.expired() is True


@patch("pyraftkv.raft.timer.time.monotonic")
@patch("pyraftkv.raft.timer.random.uniform")
def test_reset_extends_deadline(
    mock_uniform,
    mock_monotonic,
):
    mock_uniform.return_value = 0.2
    mock_monotonic.return_value = 10.0

    timer = ElectionTimer()

    first_deadline = timer._deadline

    mock_monotonic.return_value = 10.1
    timer.reset()

    assert timer._deadline > first_deadline
