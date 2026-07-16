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
    
    def get_processing_rank(self):
        """Returns current processing rank"""
        return self._processing_rank
    
    def remaining_data(self):
        """Returns remaining elements in storage number"""
        return len(self._internal_data)

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


class DataStream():
    """Can register different types of DataProcessors and
    routes received stream of data to the appropiate one"""

    def __init__(self):
        self._registered_processors: dict[str, DataProcessor] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        if proc.__class__.__name__ in self._registered_processors.keys():
            while True:
                try:
                    answer: str = input(
                        f"Warning: there's already a {proc.__class__.__name__} registered",
                        "do you wish to overwrite it [y/n]:"
                    )
                except EOFError:
                    print(
                        "\nError: input stream was closed.",
                        f"Cancelling {proc.__class.__name__} override"
                    )
                    return None
                else:
                    if answer == "n":
                        print(f"Cancelling {proc.__class.__name__} override")
                        return None
                    elif answer not in ["y", "n"]:
                        print(f"Error: action '{answer}' is not recognized")
                        continue
        if isinstance(proc, NumericProcessor):
            self._registered_processors[proc.__class__.__name__] = proc
        elif isinstance(proc, TextProcessor):
            self._registered_processors[proc.__class__.__name__] = proc
        elif isinstance(proc, LogProcessor):
            self._registered_processors[proc.__class__.__name__] = proc
        
    def process_stream(self, stream: list[Any]) -> None:
        """Analyzes each received element
        and sends it to the appropiate registered data processor"""

        for element in stream:
            processed: bool = False
            for processor in self._registered_processors.values():
                if processor.validate(element):
                    processor.ingest(element)
                    processed = True
            if not processed:
                print(
                    "DataStream error - Can't process element in stream:",
                    element
                )

    def print_processor_stats(self) -> None:
        """Prints stream statistics"""

        print("== DataStream statistics ==")
        for name, processor in self._registered_processors.items():
            print(
                f"{name}: total {processor.get_processing_rank()} items processed,",
                f"remaining {processor.remaining_data()}"
            )
