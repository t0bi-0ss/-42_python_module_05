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
            return -1, "No data: Internal data is empty"
        
        
class NumericProcessor(DataProcessor):
    """DataProcessor subclass for numeric data processing"""

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float, list)):
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, (int, float)):
                        return False
            return True
        else:
            return False
        
    def ingest(self, data: int | float | list[int | float]) -> None:
        try:
            if isinstance(data, list):
                for item in data:
                    int(item)
            else:
                int(data)
        except (ValueError, TypeError):
            print("Got exception: Improper numeric data")
        else:
            if isinstance(data, list):
                self._internal_data.append((self._processing_rank, [str(item) for item in data]))
            else:
                self._internal_data.append((self._processing_rank, str(data)))
            self._processing_rank += 1


class TextProcessor(DataProcessor):
    """DataProcessor subclass for text data processing"""

    def validate(self, data: Any) -> bool:
        if isinstance(data, (str, list)):
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, str):
                        return False
            return True
        else:
            return False
        
    def ingest(self, data: str | list[str]) -> None:
        try:
            if isinstance(data, list):
                for item in data:
                    item + "abc"
            data + "abc"
        except TypeError:
            print("Got exception: Improper text data")
        else:
            self._internal_data.append((self._processing_rank, data))
            self._processing_rank += 1


class LogProcessor(DataProcessor):
    """DataProcessor subclass for log data processing"""

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key in data.keys():
                if not isinstance(key, str):
                    return False
            for value in data.values():
                if not isinstance(value, str):
                    return False
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict) or not self.validate(item):
                    return False
            return True
        else:
            return False
        
    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        try:
            if isinstance(data, list):
                for item in data:
                    item.keys()
            else:
                data.keys()
        except AttributeError:
            print("Got exception: Improper log data")
        else:
            self._internal_data.append(self._processing_rank, data)
            self._processing_rank += 1
