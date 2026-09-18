import pytest

from pyraftkv.raft.log import RaftCommand, RaftLog
from pyraftkv.raft.state import RaftState
from pyraftkv.raft.state_machine import (
    apply_command,
    apply_committed_entries,
)
from pyraftkv.storage.store import KVStore


def test_committed_put_is_applied():
    state = RaftState(
        node_id="node-1",
        commit_index=1,
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    store = KVStore()

    apply_committed_entries(
        state,
        log,
        store,
    )

    assert store.get("name") == "harsha"
    assert state.last_applied == 1


def test_uncommitted_entry_is_not_applied():
    state = RaftState(
        node_id="node-1",
        commit_index=0,
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    store = KVStore()

    apply_committed_entries(
        state,
        log,
        store,
    )

    assert store.get("name") is None
    assert state.last_applied == 0

def test_committed_delete_is_applied():
    state = RaftState(
        node_id="node-1",
        commit_index=2,
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    log.append(
        term=1,
        command=RaftCommand(
            operation="DELETE",
            key="name",
        ),
    )

    store = KVStore()

    apply_committed_entries(
        state,
        log,
        store,
    )

    assert store.get("name") is None
    assert state.last_applied == 2


def test_entries_are_applied_in_order():
    state = RaftState(
        node_id="node-1",
        commit_index=3,
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="java",
        ),
    )

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    store = KVStore()

    apply_committed_entries(
        state,
        log,
        store,
    )

    assert store.get("language") == "python"
    assert store.get("name") == "harsha"

    assert state.last_applied == 3

def test_already_applied_entries_are_skipped():
    state = RaftState(
        node_id="node-1",
        commit_index=1,
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="x",
            value="10",
        ),
    )

    store = KVStore()

    apply_committed_entries(state, log, store)

    assert state.last_applied == 1

    apply_committed_entries(state, log, store)

    assert state.last_applied == 1
    assert store.get("x") == "10"

def test_unknown_command_is_rejected():
    store = KVStore()

    command = RaftCommand(
        operation="INVALID",
        key="x",
    )

    with pytest.raises(ValueError):
        apply_command(store, command)

        