from pyraftkv.raft.log import LogEntry, RaftCommand
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    TimeoutNowRequest,
)
from pyraftkv.transport.serialization import (
    append_entries_from_dict,
    append_entries_to_dict,
    timeout_now_from_dict,
    timeout_now_to_dict,
)


def test_append_entries_serialization_round_trip():
    request = AppendEntriesRequest(
        term=3,
        leader_id="node-1",
        prev_log_index=4,
        prev_log_term=2,
        entries=(
            LogEntry(
                index=5,
                term=3,
                command=RaftCommand(
                    operation="PUT",
                    key="language",
                    value="python",
                ),
            ),
        ),
        leader_commit=4,
    )

    data = append_entries_to_dict(request)

    restored = append_entries_from_dict(data)

    assert restored == request


def test_timeout_now_serialization_round_trip():
    request = TimeoutNowRequest(
        term=4,
        leader_id="node-1",
    )

    assert timeout_now_from_dict(
        timeout_now_to_dict(request)
    ) == request
