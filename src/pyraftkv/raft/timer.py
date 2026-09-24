import random
import time


class ElectionTimer:
    def __init__(
        self,
        min_timeout: float = 1.5,
        max_timeout: float = 3.0,
    ) -> None:
        if min_timeout <= 0:
            raise ValueError("min_timeout must be positive")

        if max_timeout <= min_timeout:
            raise ValueError("max_timeout must be greater than min_timeout")

        self.min_timeout = min_timeout
        self.max_timeout = max_timeout

        self._deadline = 0.0
        self.reset()

    def reset(self) -> None:
        timeout = random.uniform(
            self.min_timeout,
            self.max_timeout,
        )

        self._deadline = time.monotonic() + timeout

    def expired(self) -> bool:
        return time.monotonic() >= self._deadline

    @property
    def remaining(self) -> float:
        return max(
            0.0,
            self._deadline - time.monotonic(),
        )

    def expire_now(self) -> None:
        self._deadline = 0.0
