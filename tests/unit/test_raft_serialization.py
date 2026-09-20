from pyraftkv.raft.log import LogEntry, RaftCommand
from pyraftkv.raft.rpc import AppendEntriesRequest
from pyraftkv.transport.serialization import (
    append_entries_from_dict,
    append_entries_to_dict,
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
