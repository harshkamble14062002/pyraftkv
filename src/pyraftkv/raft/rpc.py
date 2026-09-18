from dataclasses import dataclass


@dataclass(frozen=True)
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: tuple = ()
    leader_commit: int = 0


@dataclass(frozen=True)
class AppendEntriesResponse:
    term: int
    success: bool
