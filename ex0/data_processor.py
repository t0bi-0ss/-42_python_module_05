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
                for item in data:
                    self._internal_data.append((self._processing_rank, str(item)))
                    self._processing_rank += 1
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
            else:
                data + "abc"
        except TypeError:
            print("Got exception: Improper text data")
        else:
            if isinstance(data, list):
                for string in data:
                    self._internal_data.append((self._processing_rank, string))
                    self._processing_rank += 1
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
                    for key, value in item.items():
                        key + "abc"
                        value + "abc"
            else:
                data.keys()
                for key, value in data.items():
                    key + "abc"
                    value + "abc"
        except (TypeError, AttributeError):
            print("Got exception: Improper log data")
        else:
            if isinstance(data, list):
                for item in data:
                    self._internal_data.append((self._processing_rank, item))
                    self._processing_rank += 1
            else:
                self._internal_data.append(self._processing_rank, data)
                self._processing_rank += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data processor ===\n")

    print("---> Testing Numeric Processor...")
    # Declare NumericProcessor class object
    num_processor = NumericProcessor()
    print("Trying to validate input '42':", num_processor.validate("42"))
    print("Trying to validate input 'Hello':", num_processor.validate("Hello"))
    print("Test invalid ingestion of string 'foo' without prior validation:")
    num_processor.ingest("foo")
    print("Processing data: [1, 2, 3, 4, 5]")
    num_processor.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for i in range(0, 3):
        extracted = num_processor.ouput()
        print(
            "Numeric value",
            f"{extracted[0]}: {extracted[1]}"
        )
    print("\n>>>Additional tests<<<")
    print("Trying to validate input [1, 'a', 3]:", num_processor.validate([1, 'a', 3]))
    print("Test invalid ingestion of list [1, 'a', 3] without prior validation:")
    num_processor.ingest([1, 'a', 3])
    print("Extracting all values even when there's none")
    for i in range(0, 5):
        extracted = num_processor.ouput()
        print(
            "Numeric value",
            f"{extracted[0]}: {extracted[1]}"
        )
    
    print("\n---> Testing Text Processor...")
    # Declare TextProcessor class object
    text_processor = TextProcessor()
    print("Trying to validate input '42':", text_processor.validate(42))
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text_processor.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    extracted = text_processor.ouput()
    print(f"Text value {extracted[0]}: {extracted[1]}")
    print("\n>>>Additional tests<<<")
    print("Test invalid ingestion of list ['Hello', 'World', 1, 'Sup']:")
    text_processor.ingest(['Hello', 'World', 1, 'Sup'])
    print("Test invalid ingestion of list ['Hello', 'World', ['What'], 'Sup']:")
    text_processor.ingest(['Hello', 'World', ['What'], 'Sup'])


    print("\n---> Testing Log Processor...")
    # Declare LogProcessor class object
    log_processor = LogProcessor()
    print("Trying to validate input 'Hello':", log_processor.validate("Hello"))
    to_process = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
        ]
    print("Processing data:", to_process)
    log_processor.ingest(to_process)
    print("Extracting 2 values...")
    for i in range(0, 2):
        extracted = log_processor.ouput()
        print(
            f"Log entry {extracted[0]}: {extracted[1]['log_level']}:",
            extracted[1]['log_message']
        )
    print("\n>>>Additional tests<<<")
    to_process = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {1: 'ERROR', 'log_message': 'Unauthorized access!!'}
        ]
    print("Test invalid ingestion of", to_process)
    log_processor.ingest(to_process)
