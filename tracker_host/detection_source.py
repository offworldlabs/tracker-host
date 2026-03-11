"""DetectionSource protocol for pluggable detection ingestion."""

from typing import Any, Optional, Protocol


class DetectionSource(Protocol):
    async def receive(self) -> Optional[dict[str, Any]]:
        """Return next detection frame, or None if no new data."""
        ...

    async def close(self) -> None: ...

    @property
    def is_healthy(self) -> bool: ...
