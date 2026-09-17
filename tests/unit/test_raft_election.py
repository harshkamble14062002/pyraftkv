from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.rpc import RequestVoteResponse
from pyraftkv.raft.state import NodeRole, RaftState


def test_three_node_cluster_requires_two_votes():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    assert election.quorum_size == 2


def test_start_election_votes_for_self():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    election.start()

    assert state.role == NodeRole.CANDIDATE
    assert state.voted_for == "node-1"
    assert election.votes_received == {"node-1"}


def test_candidate_becomes_leader_after_majority():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    election.start()

    became_leader = election.record_vote(
        "node-2",
        RequestVoteResponse(
            term=state.current_term,
            vote_granted=True,
        ),
    )

    assert became_leader is True
    assert state.role == NodeRole.LEADER
    assert state.leader_id == "node-1"


def test_duplicate_vote_is_not_counted_twice():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {
            "node-1",
            "node-2",
            "node-3",
            "node-4",
            "node-5",
        },
    )

    election.start()

    response = RequestVoteResponse(
        term=state.current_term,
        vote_granted=True,
    )

    election.record_vote("node-2", response)
    election.record_vote("node-2", response)

    assert len(election.votes_received) == 2
    assert state.role == NodeRole.CANDIDATE


def test_rejected_vote_does_not_count():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    election.start()

    election.record_vote(
        "node-2",
        RequestVoteResponse(
            term=state.current_term,
            vote_granted=False,
        ),
    )

    assert election.votes_received == {"node-1"}
    assert state.role == NodeRole.CANDIDATE


def test_higher_term_response_forces_step_down():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    election.start()

    election.record_vote(
        "node-2",
        RequestVoteResponse(
            term=state.current_term + 1,
            vote_granted=False,
        ),
    )

    assert state.role == NodeRole.FOLLOWER
    assert election.active is False


def test_unknown_node_vote_is_ignored():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    election.start()

    election.record_vote(
        "fake-node",
        RequestVoteResponse(
            term=state.current_term,
            vote_granted=True,
        ),
    )

    assert election.votes_received == {"node-1"}


def test_single_node_cluster_elects_itself():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1"},
    )

    election.start()

    assert state.role == NodeRole.LEADER
