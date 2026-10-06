import asyncio
import time
from collections import defaultdict, deque
from typing import Deque, Dict


class RateLimitError(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__("Too many requests.")


class RateLimiter:

    def __init__(
        self,
        max_requests: int,
        window_seconds: int,
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

        self._requests: Dict[
            str,
            Deque[float],
        ] = defaultdict(deque)

    def check(self, key: str) -> None:

        now = time.monotonic()

        request_times = self._requests[key]

        cutoff = now - self.window_seconds

        while (
            request_times
            and request_times[0] <= cutoff
        ):
            request_times.popleft()

        if len(request_times) >= self.max_requests:

            retry_after = max(
                1,
                int(
                    self.window_seconds
                    - (now - request_times[0])
                ),
            )

            raise RateLimitError(
                retry_after
            )

        request_times.append(now)


# Login protection
login_limiter = RateLimiter(
    max_requests=5,
    window_seconds=60,
)


# New conversation protection
chat_limiter = RateLimiter(
    max_requests=20,
    window_seconds=60,
)


# Follow-up protection
followup_limiter = RateLimiter(
    max_requests=30,
    window_seconds=60,
)


# Limit the number of Genie requests
# executing simultaneously inside one
# FastAPI process.
genie_semaphore = asyncio.Semaphore(5)