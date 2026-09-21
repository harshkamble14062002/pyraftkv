from typing import Any

from pyraftkv.raft.log import LogEntry, RaftCommand
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


def request_vote_to_dict(
    request: RequestVoteRequest,
) -> dict[str, Any]:
    return {
        "term": request.term,
        "candidate_id": request.candidate_id,
        "last_log_index": request.last_log_index,
        "last_log_term": request.last_log_term,
    }


def request_vote_from_dict(
    data: dict[str, Any],
) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=int(data["term"]),
        candidate_id=str(data["candidate_id"]),
        last_log_index=int(data["last_log_index"]),
        last_log_term=int(data["last_log_term"]),
    )


def request_vote_response_to_dict(
    response: RequestVoteResponse,
) -> dict[str, Any]:
    return {
        "term": response.term,
        "vote_granted": response.vote_granted,
    }


def request_vote_response_from_dict(
    data: dict[str, Any],
) -> RequestVoteResponse:
    return RequestVoteResponse(
        term=int(data["term"]),
        vote_granted=bool(data["vote_granted"]),
    )


def command_to_dict(
    command: RaftCommand,
) -> dict[str, Any]:
    return {
        "operation": command.operation,
        "key": command.key,
        "value": command.value,
    }


def command_from_dict(
    data: dict[str, Any],
) -> RaftCommand:
    value = data.get("value")

    if value is not None:
        value = str(value)

    return RaftCommand(
        operation=str(data["operation"]),
        key=str(data["key"]),
        value=value,
    )


def log_entry_to_dict(
    entry: LogEntry,
) -> dict[str, Any]:
    return {
        "index": entry.index,
        "term": entry.term,
        "command": command_to_dict(entry.command),
    }


def log_entry_from_dict(
    data: dict[str, Any],
) -> LogEntry:
    return LogEntry(
        index=int(data["index"]),
        term=int(data["term"]),
        command=command_from_dict(data["command"]),
    )


def append_entries_to_dict(
    request: AppendEntriesRequest,
) -> dict[str, Any]:
    return {
        "term": request.term,
        "leader_id": request.leader_id,
        "prev_log_index": request.prev_log_index,
        "prev_log_term": request.prev_log_term,
        "entries": [log_entry_to_dict(entry) for entry in request.entries],
        "leader_commit": request.leader_commit,
    }


def append_entries_from_dict(
    data: dict[str, Any],
) -> AppendEntriesRequest:
    return AppendEntriesRequest(
        term=int(data["term"]),
        leader_id=str(data["leader_id"]),
        prev_log_index=int(data["prev_log_index"]),
        prev_log_term=int(data["prev_log_term"]),
        entries=tuple(log_entry_from_dict(entry) for entry in data.get("entries", [])),
        leader_commit=int(data["leader_commit"]),
    )


def append_entries_response_to_dict(
    response: AppendEntriesResponse,
) -> dict[str, Any]:
    return {
        "term": response.term,
        "success": response.success,
    }


def append_entries_response_from_dict(
    data: dict[str, Any],
) -> AppendEntriesResponse:
    return AppendEntriesResponse(
        term=int(data["term"]),
        success=bool(data["success"]),
    )
