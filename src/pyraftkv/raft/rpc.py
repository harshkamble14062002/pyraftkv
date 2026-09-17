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
