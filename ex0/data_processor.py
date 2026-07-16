from abc import ABC, abstractmethod

from typing import Any

"""Module that demonstrates a basic use of abstract classes"""


class DataProcessor(ABC):
    """Abstract class for data processing"""

    def __init__(self) -> None:
        self._internal_data: list[tuple[int, Any]] = []
        self._processing_rank = 0

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Processes input data"""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Checks if input data is appropiate"""
        pass

    def ouput(self) -> tuple[int, str]:
        """Outputs ingested data"""
        if len(self._internal_data) > 0:
            return self._internal_data.pop(0)
        else:
            return -1, "Internal data is empty"
