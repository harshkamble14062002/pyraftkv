from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from pyraftkv.raft.command_processor import RaftCommandProcessor
from pyraftkv.raft.log import RaftCommand


def test_processor_batches_concurrent_commands():
    node = Mock()
    node.node_id = "node-1"
    node.submit_commands.return_value = [True, True]
    transport = Mock()

    processor = RaftCommandProcessor(
        node,
        transport,
        batch_wait_seconds=0.05,
    )
    processor.start()

    commands = [
        RaftCommand(
            operation="PUT",
            key=f"key-{index}",
            value=f"value-{index}",
        )
        for index in range(2)
    ]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(processor.submit, commands))
    finally:
        processor.stop()

    assert results == [True, True]
    node.submit_commands.assert_called_once_with(
        commands,
        transport,
    )


def test_processor_propagates_submission_errors():
    node = Mock()
    node.node_id = "node-1"
    node.submit_commands.side_effect = RuntimeError("failed")

    processor = RaftCommandProcessor(
        node,
        Mock(),
    )
    processor.start()

    try:
        try:
            processor.submit(
                RaftCommand(
                    operation="PUT",
                    key="key",
                    value="value",
                )
            )
        except RuntimeError as exc:
            assert str(exc) == "failed"
        else:
            raise AssertionError("Expected RuntimeError")
    finally:
        processor.stop()
