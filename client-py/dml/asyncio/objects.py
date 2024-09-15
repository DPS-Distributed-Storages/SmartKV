from typing import Any, Optional, List, TYPE_CHECKING, Union

from dml.storage.commands import CommandFlag

if TYPE_CHECKING:
    # avoid circular import
    from dml.asyncio.client import DmlClient


class SharedObject:
    """
    Base class for shared objects.
    """

    def __init__(self, dml: 'DmlClient', key: str) -> None:
        self.dml = dml
        self.key = key
        self.lock_token = None

    async def _invoke_method(self, method_name: str, args: List[Any] = None, read_only=False,
                             eventual_cst=False) -> Any:
        if read_only:
            flags = CommandFlag.READ_ONLY | CommandFlag.ALLOW_INVALID_READS if eventual_cst else CommandFlag.READ_ONLY
        else:
            flags = CommandFlag.ASYNC_REPLICATION if eventual_cst else CommandFlag.NONE
        return await self.dml.invoke_method(self.key, method_name, args=args, lock_token=self.lock_token, flags=flags)

    async def lock(self) -> None:
        """
        Acquires a lock for the object.
        """
        self.lock_token = await self.dml.lock(self.key)

    async def unlock(self) -> None:
        """
        Unlocks the object.
        """
        await self.dml.unlock(self.key, self.lock_token)
        self.lock_token = None

    async def delete(self) -> None:
        """
        Deletes the object.
        """
        await self.dml.delete(self.key)


class SharedBuffer(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str) -> None:
        """
        Creates a proxy object for an existing shared buffer.
        Use the :func:`dml.asyncio.objects.SharedBuffer.create` method to create a new shared buffer.
        :param dml: the DML client
        :param key: the name of the JSON document
        """
        super().__init__(dml, key)

    @classmethod
    async def create(cls, dml: 'DmlClient', key: str, value: Optional[Union[bytes, bytearray]] = None,
                     replicas: Optional[List[int]] = None) -> 'SharedBuffer':
        """
        Creates a new shared buffer and returns a proxy object for it. If a buffer with the same name already
        exists, a proxy for the existing buffer is returned.
        :param dml: the DML client
        :param key: the name of the document
        :param value: the initial value of the buffer
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        :return: a proxy object for the buffer
        """
        self = SharedBuffer(dml, key)
        await self.dml.create(key, object_type='SharedBuffer',
                              args=[value] if value is not None else None,
                              replicas=replicas)
        return self

    async def get(self, eventual_cst: bool = False) -> bytes:
        """
        Returns the current value of the buffer.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the current value of the buffer
        """
        return await self._invoke_method('get', read_only=True, eventual_cst=eventual_cst)

    async def set(self, value: Optional[Union[bytes, bytearray]], eventual_cst: bool = False) -> None:
        """
        Sets the buffer to the given value.
        :param value: the value
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('set', args=[value], eventual_cst=eventual_cst)


class SharedCounter(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str) -> None:
        """
        Creates a proxy object for an existing shared counter.
        Use the :func:`dml.asyncio.objects.SharedCounter.create` method to create a new shared counter.
        :param dml: the DML client
        :param key: the name of the shared counter
        """
        super().__init__(dml, key)

    @classmethod
    async def create(cls, dml: 'DmlClient', key: str, value: int = 0,
                     replicas: Optional[List[int]] = None) -> 'SharedCounter':
        """
        Creates a new shared counter and returns a proxy object for it. If a counter with the same name already
        exists, a proxy for the existing counter is returned.
        :param dml: the DML client
        :param key: the name of the counter
        :param value: the initial value of the counter
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        :return: a proxy object for the counter
        """
        self = SharedCounter(dml, key)
        await self.dml.create(key, object_type='SharedCounter', args=[value], replicas=replicas)
        return self

    async def get(self, eventual_cst: bool = False) -> int:
        """
        Returns the current value of the counter.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the current value of the counter
        """
        return await self._invoke_method('get', read_only=True, eventual_cst=eventual_cst)

    async def set(self, value: int = 0, eventual_cst: bool = False) -> None:
        """
        Sets the counter to the given delta.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :param value: the value
        """
        await self._invoke_method('set', args=[value], eventual_cst=eventual_cst)

    async def increment(self, delta: int = 1, eventual_cst: bool = False) -> int:
        """
        Increments the counter by the given delta.
        :param delta: the value to increment the counter by
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the new value of the counter
        """
        return await self._invoke_method('increment', args=[delta], eventual_cst=eventual_cst)

    async def decrement(self, delta: int = 1, eventual_cst: bool = False) -> int:
        """
        Decrements the counter by the given delta.
        :param delta: the value to decrement the counter by
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the new value of the counter
        """
        return await self._invoke_method('decrement', args=[delta], eventual_cst=eventual_cst)


class SharedJson(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str) -> None:
        """
        Creates a proxy object for an existing shared JSON document.
        Use the :func:`dml.asyncio.objects.SharedJson.create` method to create a new shared JSON document.
        :param dml: the DML client
        :param key: the name of the JSON document
        """
        super().__init__(dml, key)

    @classmethod
    async def create(cls, dml: 'DmlClient', key: str, json: Optional[Any] = None,
                     replicas: Optional[List[int]] = None) -> 'SharedJson':
        """
        Creates a new shared JSON document and returns a proxy object for it. If a document with the same name already
        exists, a proxy for the existing document is returned.
        :param dml: the DML client
        :param key: the name of the document
        :param json: the initial content of the JSON document
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        :return: a proxy object for the JSON document
        """
        self = SharedJson(dml, key)
        await self.dml.create(key, object_type='SharedJson',
                              args=[json] if json is not None else None,
                              replicas=replicas)
        return self

    async def set(self, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Sets the value at the given path.
        :param value: the value to be set
        :param path: the JSONPath to the value to be set
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('set', args=[path, value], eventual_cst=eventual_cst)

    async def get(self, path: str = '$', eventual_cst: bool = False) -> Any:
        """
        Returns the value at the given path.
        :param path: the JSONPath to the value to be returned
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the value at the given path
        """
        return await self._invoke_method('get', args=[path], read_only=True, eventual_cst=eventual_cst)

    async def put(self, key: str, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Adds or updates the key with the given value at the given path.
        :param key: the key to add or update
        :param value: the value to add or update
        :param path: the JSONPath to the key
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('put', args=[path, key, value], eventual_cst=eventual_cst)

    async def add(self, value: Any, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Adds the given value to the array at the given path.
        :param value: the value to add
        :param path: the JSONPath to the array
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('add', args=[path, value], eventual_cst=eventual_cst)

    async def delete(self, path: str = '$', eventual_cst: bool = False) -> None:
        """
        Deletes the value at the given path.
        :param path: the JSONPath to the value to be deleted
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('delete', args=[path], eventual_cst=eventual_cst)

    async def stringify(self, eventual_cst: bool = False) -> str:
        """
        Returns a string representation of the JSON document.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: a string representation of the JSON document
        """
        return await self._invoke_method('getAsString', read_only=True, eventual_cst=eventual_cst)


class SharedClassDef(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str) -> None:
        """
        Creates a proxy object for an existing shared class definition object.
        Use the :func:`dml.asyncio.objects.SharedClassDef.create` method to create a new shared class definition.
        :param dml: the DML client
        :param key: the name of the JSON document
        """
        super().__init__(dml, key)

    @classmethod
    async def create(cls, dml: 'DmlClient', class_name: str, language: str, byte_code: bytes,
                     replicas: Optional[List[int]] = None, full_replication: bool = False) -> 'SharedClassDef':
        """
        Creates a new shared class definition and returns a proxy object for it. If a class definition with the same name already
        exists, a proxy for the existing definition is returned.
        :param dml: the DML client
        :param class_name: the name of the class / prototype to be defined
        :param language: the programming language in which the class / prototype is implemented (e.g. java or lua)
        :param byte_code: the code of the class as bytes
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        :param full_replication: if True, the object is replicated on each storage node
        :return: a proxy object for the buffer
        """
        self = SharedClassDef(dml, class_name)
        await self.dml.create(class_name, object_type='SharedClassDef',
                              args=[byte_code, class_name, language],
                              replicas=replicas, full_replication=full_replication)
        return self

    async def get(self, eventual_cst: bool = False) -> bytes:
        """
        Returns the bytes of the byte_code.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the bytes of the byte_code
        """
        return await self._invoke_method('get', read_only=True, eventual_cst=eventual_cst)

    async def set(self, byte_code: bytes, eventual_cst: bool = False) -> None:
        """
        Sets the byte_code to the given bytes.
        :param byte_code: the byte_code
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('set', args=[byte_code], eventual_cst=eventual_cst)

    async def get_class_name(self, eventual_cst: bool = False) -> str:
        """
        Returns the class name of the class definition.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the class name of the class definition
        """
        return await self._invoke_method('getClassName', read_only=True, eventual_cst=eventual_cst)


class SharedStatistics(SharedObject):
    def __init__(self, dml: 'DmlClient', key: str) -> None:
        """
        Creates a proxy object for an existing shared statistic object.
        Use the :func:`dml.asyncio.objects.SharedStatistics.create` method to create a new shared statistic object.
        :param dml: the DML client
        :param key: the name of the JSON document
        """
        super().__init__(dml, key)

    @classmethod
    async def create(cls, dml: 'DmlClient', key: str, replicas: Optional[List[int]] = None,
                     full_replication: bool = False) -> 'SharedStatistics':
        """
        Creates a new shared class definition and returns a proxy object for it. If a class definition with the same name already
        exists, a proxy for the existing definition is returned.
        :param dml: the DML client
        :param key: the name/key of the object
        :param replicas: the node IDs storing replicas or None if the replicas should be selected automatically
        :param full_replication: if True, the object is replicated on each storage node
        :return: a proxy object for the buffer
        """
        self = SharedStatistics(dml, key)
        await self.dml.create(key, object_type='SharedStatistics',
                              args=None,
                              replicas=replicas, full_replication=full_replication)
        return self

    async def get(self, eventual_cst: bool = False) -> Any:
        """
        Returns the statistics object.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :return: the statistics object.
        """
        return await self._invoke_method('get', read_only=True, eventual_cst=eventual_cst)

    async def set(self, statistics: List[int], eventual_cst: bool = False) -> None:
        """
        Sets the statistics to the given list of integers.
        :param statistics: the list of statistic values
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        """
        await self._invoke_method('set', args=[statistics], eventual_cst=eventual_cst)

    async def add(self, value: int, eventual_cst: bool = False) -> bool:
        """
        Adds a statistic element to the statistics.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :param value: the element which shall be added to the statistics
        :return: true if the element was added successfully.
        """
        return await self._invoke_method('add', args=[value], read_only=False, eventual_cst=eventual_cst)

    async def remove(self, value: int, eventual_cst: bool = False) -> bool:
        """
        Removes the first occurrence of the specified element from the statistics.
        :param eventual_cst: if True, the command is executed in eventual consistency mode instead of strong consistency
        :param value: the element which shall be removed from the statistics
        :return: true if the element was contained and removed successfully.
        """
        return await self._invoke_method('remove', args=[value], read_only=False, eventual_cst=eventual_cst)

    async def get_statistics(self, eventual_cst: bool = False) -> List[int]:
        """
        Returns the statistics as List of ints.
        :return: a list of statistic values.
        """
        return await self._invoke_method('getStatistics', read_only=True, eventual_cst=eventual_cst)

    async def average(self, eventual_cst: bool = False) -> float:
        """
        Calculates and returns an average over all statistic elements.
        :return: the average.
        """
        return await self._invoke_method('average', read_only=True, eventual_cst=eventual_cst)
