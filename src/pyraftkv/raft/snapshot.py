from dataclasses import dataclass, field


@dataclass(frozen=True)
class RaftSnapshot:
    last_included_index: int = 0
    last_included_term: int = 0
    state: dict[str, str] = field(default_factory=dict)
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError(
                f"Unsupported Raft snapshot version: {self.version}"
            )

        if self.last_included_index < 0:
            raise ValueError(
                "last_included_index cannot be negative"
            )

        if self.last_included_term < 0:
            raise ValueError(
                "last_included_term cannot be negative"
            )

        if (
            self.last_included_index == 0
            and self.last_included_term != 0
        ):
            raise ValueError(
                "last_included_term must be zero at index zero"
            )
