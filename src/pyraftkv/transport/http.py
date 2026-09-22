import httpx

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
)
from pyraftkv.transport.base import TransportError
from pyraftkv.transport.serialization import (
    append_entries_response_from_dict,
    append_entries_to_dict,
    request_vote_response_from_dict,
    request_vote_to_dict,
    timeout_now_response_from_dict,
    timeout_now_to_dict,
)


class HTTPTransport:
    def __init__(
        self,
        addresses: dict[str, str],
        timeout: float = 0.2,
        client: httpx.Client | None = None,
    ) -> None:
        self._addresses = {
            node_id: address.rstrip("/") for node_id, address in addresses.items()
        }

        self._timeout = timeout

        self._client = client or httpx.Client(timeout=timeout)

        self._owns_client = client is None

    def _url(
        self,
        target_id: str,
        path: str,
    ) -> str:
        address = self._addresses.get(target_id)

        if address is None:
            raise TransportError(f"Unknown node: {target_id}")

        return f"{address}{path}"

    def request_vote(
        self,
        target_id: str,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        url = self._url(
            target_id,
            "/raft/request-vote",
        )

        try:
            response = self._client.post(
                url,
                json=request_vote_to_dict(request),
            )

            response.raise_for_status()

        except httpx.HTTPError as exc:
            raise TransportError(f"RequestVote to {target_id} failed") from exc

        try:
            return request_vote_response_from_dict(response.json())
        except (KeyError, TypeError, ValueError) as exc:
            raise TransportError(
                f"Invalid RequestVote response from {target_id}"
            ) from exc

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        url = self._url(
            target_id,
            "/raft/append-entries",
        )

        try:
            response = self._client.post(
                url,
                json=append_entries_to_dict(request),
            )

            response.raise_for_status()

        except httpx.HTTPError as exc:
            raise TransportError(f"AppendEntries to {target_id} failed") from exc

        try:
            return append_entries_response_from_dict(response.json())
        except (KeyError, TypeError, ValueError) as exc:
            raise TransportError(
                f"Invalid AppendEntries response from {target_id}"
            ) from exc

    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        url = self._url(
            target_id,
            "/raft/timeout-now",
        )

        try:
            response = self._client.post(
                url,
                json=timeout_now_to_dict(request),
            )

            response.raise_for_status()

        except httpx.HTTPError as exc:
            raise TransportError(
                f"TimeoutNow to {target_id} failed"
            ) from exc

        try:
            return timeout_now_response_from_dict(
                response.json()
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TransportError(
                f"Invalid TimeoutNow response from {target_id}"
            ) from exc

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
