from abc import ABC, abstractmethod

from typing import Any

"""Module that demonstrates a basic use of abstract classes"""


class DataProcessor(ABC):
    """Abstract class for data processing"""

    internal_data: list[tuple[int, Any]] = []

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @classmethod
    def ouput(self) -> tuple[int, str]:

