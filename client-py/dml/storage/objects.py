from __future__ import annotations

import time
from typing import Optional, List, Any, Union, TYPE_CHECKING

import bson

from dml.storage.commands import CommandFlag

if TYPE_CHECKING:
    # avoid circular import
    from dml.client import DmlClient


class BsonArgsCodec:
    @staticmethod
    def encode(args: List[Any]) -> bytes:
        # bson.encode requires a mapping type, so we wrap the args in a dict with the indices as keys
        args_dict = {str(k): v for k, v in enumerate(args)}
        return bson.encode(args_dict)

    @staticmethod
    def decode(encoded_args: Union[bytes, bytearray]) -> List[Any]:
        # bson.decode returns a dictionary instead of an array, so we return the values of the dict in a list
        return list(bson.decode(encoded_args).values())


class SharedObject:
    """
    Base class for shared objects.
    """

    def __init__(self, dml: 'DmlClient', object_type: str, key: str, args: List[Any] = None,
                 replicas: Optional[List[int]] = None) -> None:
        self.dml = dml
        self.key = key
        self.lock_token = None
        self.dml.create(key, object_type=object_type, args=args, replicas=replicas)

    def _invoke_method(self, method_name: str, args: List[Any] = None, read_only=False,
                       eventual_cst: bool = False) -> Any:
        if read_only:
            flags = CommandFlag.READ_ONLY | CommandFlag.ALLOW_INVALID_READS if eventual_cst else CommandFlag.READ_ONLY
        else:
            flags = CommandFlag.ASYNC_REPLICATION if eventual_cst else CommandFlag.NONE
        return self.dml.invoke_method(self.key, method_name, args=args, lock_token=self.lock_token, flags=flags)

    def _set(self, args: List[Any] = None, eventual_cst: bool = False) -> Any:
        return self.dml.set(self.key, values=args, lock_token=self.lock_token, async_replication=eventual_cst)

    def _get(self, allow_invalid_reads: bool = False) -> Any:
        return self.dml.get(self.key, lock_token=self.lock_token, allow_invalid_reads=allow_invalid_reads)

    def lock(self) -> None:
        """
        Acquires a lock for the object.
        """
        self.lock_token = self.dml.lock(self.key)

    def unlock(self) -> None:
        """
        Unlocks the object.
        """
        self.dml.unlock(self.key, self.lock_token)
        self.lock_token = None

    def delete(self) -> None:
        """
        Deletes the object.
        """
        self.dml.delete(self.key)


class SharedBuffer(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str, value: Optional[Union[bytes, bytearray]] = None,
                 replicas: Optional[List[int]] = None) -> None:
        super().__init__(dml, 'SharedBuffer', key, args=[value] if value is not None else None, replicas=replicas)

    def get(self, eventual_cst: bool = False) -> bytes:
        """
        Returns the current value of the buffer.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the current value of the buffer
        """
        return self._get(allow_invalid_reads=eventual_cst)["buffer"]

    def set(self, value: Optional[Union[bytes, bytearray]], eventual_cst: bool = False) -> None:
        """
        Sets the buffer to the given value.
        :param value: the value
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        return self._set(args=value, eventual_cst=eventual_cst)


class SharedCounter(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str, value: int = 0, replicas: Optional[List[int]] = None) -> None:
        super().__init__(dml, 'SharedCounter', key, args=[value], replicas=replicas)

    def get(self, eventual_cst: bool = False) -> int:
        """
        Returns the current value of the counter.
        :return: the current value of the counter
        """
        return self._get(allow_invalid_reads=eventual_cst)["value"]

    def set(self, value: int = 0, eventual_cst: bool = False) -> None:
        """
        Sets the counter to the given delta.
        :param value: the value
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._set(args=[value], eventual_cst=eventual_cst)

    def increment(self, delta: int = 1, eventual_cst: bool = False) -> int:
        """
        Increments the counter by the given delta.
        :param delta: the value to increment the counter by
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the new value of the counter
        """
        return self._invoke_method('increment', args=[delta], eventual_cst=eventual_cst)

    def decrement(self, delta: int = 1, eventual_cst: bool = False) -> int:
        """
        Decrements the counter by the given delta.
        :param delta: the value to decrement the counter by
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the new value of the counter
        """
        return self._invoke_method('decrement', args=[delta], eventual_cst=eventual_cst)


class SharedJson(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str, json: Optional[Any] = None,
                 replicas: Optional[List[int]] = None) -> None:
        """
        Creates a new shared JSON document. If a document with the same name already exists, the existing document will
        be used.
        :param dml: the DML client
        :param key: the name of the document
        :param json: the initial content of the JSON document
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        """
        super().__init__(dml, 'SharedJson', key, args=[json] if json is not None else None, replicas=replicas)

    def set(self, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Sets the value at the given path.
        :param value: the value to be set
        :param path: the JSONPath to the value to be set
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        if str == '$':
            self._set(args=[value], eventual_cst=eventual_cst)
        else:
            self._invoke_method('set', args=[path, value], eventual_cst=eventual_cst)

    def get(self, path: str = '$', eventual_cst: bool = False) -> Any:
        """
        Returns the value at the given path.
        :param path: the JSONPath to the value to be returned
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the value at the given path
        """
        if str == '$':
            return self._get(allow_invalid_reads=eventual_cst)["json"]
        else:
            return self._invoke_method('get', args=[path], read_only=True, eventual_cst=eventual_cst)

    def put(self, key: str, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Adds or updates the key with the given value at the given path.
        :param key: the key to add or update
        :param value: the value to add or update
        :param path: the JSONPath to the key
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._invoke_method('put', args=[path, key, value], eventual_cst=eventual_cst)

    def add(self, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Adds the given value to the array at the given path.
        :param value: the value to add
        :param path: the JSONPath to the array
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._invoke_method('add', args=[path, value], eventual_cst=eventual_cst)

    def delete(self, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Deletes the value at the given path.
        :param path: the JSONPath to the value to be deleted
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._invoke_method('delete', args=[path], eventual_cst=eventual_cst)

    def stringify(self, eventual_cst: bool = False) -> str:
        """
        Returns a string representation of the JSON document.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: a string representation of the JSON document
        """
        return self._invoke_method('getAsString', read_only=True, eventual_cst=eventual_cst)


class Barrier:
    def __init__(self, dml: 'DmlClient', key: str, parties: int, replicas: Optional[List[int]] = None,
                 check_delay: float = 0.1) -> None:
        self._dml = dml
        self._key = key
        self._parties = parties
        self._counter = SharedCounter(dml, key, replicas=replicas)
        self._check_delay = check_delay

    def wait(self, timeout: float = None) -> None:
        """
        Waits until all parties have called wait or the specified timeout is reached.
        :param timeout: the timeout in seconds
        """
        self._counter.increment()
        start = time.monotonic()
        while self._counter.get() % self._parties != 0:
            if timeout is not None and time.monotonic() - start > timeout:
                raise TimeoutError('Barrier wait timed out')
            time.sleep(self._check_delay)


class SharedClassDef(SharedObject):
    def __init__(self, dml: 'DmlClient', class_name: str, language: str, byte_code: bytes,
                 replicas: Optional[List[int]] = None) -> None:
        super().__init__(dml, 'SharedClassDef', class_name, args=[byte_code, class_name, language], replicas=replicas)

    def get(self, eventual_cst: bool = False) -> Any:
        """
        Returns the SharedClassDef
        :return: the SharedClassDef object
        """
        return self._get(allow_invalid_reads=eventual_cst)

    def set(self, language: str, class_name: str, byte_code: bytes, eventual_cst: bool = False) -> None:
        """
        Sets the byte_code to the given bytes.
        :param class_name: the name of the class
        :param language: the language (e.g. java or lua)
        :param byte_code: the byte_code
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._set(args=[byte_code, class_name, language], eventual_cst=eventual_cst)

    def get_class_name(self, eventual_cst: bool = False) -> str:
        """
        Returns the class name of the class definition.
        :return: the class name of the class definition
        """
        return self._invoke_method('getClassName', read_only=True, eventual_cst=eventual_cst)


class SharedStatistics(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str, replicas: Optional[List[int]] = None) -> None:
        super().__init__(dml, 'SharedStatistics', key, replicas=replicas)

    def get(self, eventual_cst: bool = False) -> List[Any]:
        """
        Returns the statistics object.
        :return: the statistics object.
        """
        return self._get(allow_invalid_reads=eventual_cst)["statistics"]

    def set(self, statistics: List[int], eventual_cst: bool = False) -> None:
        """
        Sets the statistics to the given list of integers.
        :param statistics: the list of statistic values
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        self._set(args=[statistics], eventual_cst=eventual_cst)

    def add(self, value: int, eventual_cst: bool = False) -> bool:
        """
        Adds a statistic element to the statistics.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :param value: the element which shall be added to the statistics
        :return: true if the element was added successfully.
        """
        return self._invoke_method('add', args=[value], read_only=False, eventual_cst=eventual_cst)

    def remove(self, value: int, eventual_cst: bool = False) -> bool:
        """
        Removes the first occurrence of the specified element from the statistics.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :param value: the element which shall be removed from the statistics
        :return: true if the element was contained and removed successfully.
        """
        return self._invoke_method('remove', args=[value], read_only=False, eventual_cst=eventual_cst)

    def get_statistics(self, eventual_cst: bool = False) -> List[int]:
        """
        Returns the statistics as List of ints.
        :return: a list of statistic values.
        """
        return self._invoke_method('getStatistics', read_only=True, eventual_cst=eventual_cst)

    def average(self, eventual_cst: bool = False) -> float:
        """
        Calculates and returns an average over all statistic elements.
        :return: the average.
        """
        return self._invoke_method('average', read_only=True, eventual_cst=eventual_cst)
