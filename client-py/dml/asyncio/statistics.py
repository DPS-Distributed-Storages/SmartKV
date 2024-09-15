import asyncio
import time
import uuid

from dml.asyncio.objects import SharedJson
from dml.exceptions import MetadataCommandException, MetadataCommandErrorCode


class StatisticsWriterDml:

    DEFAULT_KEY = '__clientStatistics'

    def __init__(self, dml, client_id=str(uuid.uuid4()), key=DEFAULT_KEY, max_records=6):
        self._dml = dml
        self._client_id = client_id
        self._initialized = False
        self._remote_json = SharedJson(self._dml, key)
        self._record_index = 0
        self._max_records = max_records
        self._bg_write_tasks = set()

    @property
    def dml(self):
        return self._dml

    @dml.setter
    def dml(self, dml):
        self._dml = dml
        self._remote_json.dml = dml

    async def _init_client_path(self):
        try:
            # each client has its own path in the shared JSON document
            # here we create the path for this client
            await self._remote_json.put(str(self._client_id), {})
        except MetadataCommandException as err:
            if err.num == MetadataCommandErrorCode.KEY_DOES_NOT_EXIST.value:
                # the shared JSON document does not exist yet, so we create it
                self._remote_json = \
                    await SharedJson.create(self._dml, self._remote_json.key, json={str(self._client_id): {}})
            else:
                raise

    async def _write_statistics(self, record_index, record):
        if not self._initialized:
            await self._init_client_path()
            self._initialized = True
        await self._remote_json.put(str(record_index), record, path=f'$.{self._client_id}')

    def _schedule_write_task(self, record_index, record):
        task = asyncio.create_task(self._write_statistics(record_index, record))
        self._bg_write_tasks.add(task)
        # remove the task reference from the set after completion so that the task can be garbage collected
        task.add_done_callback(self._bg_write_tasks.discard)
        return task

    def write_statistics(self, statistics, current_access_point):
        record_index = self._record_index
        self._record_index += 1
        if self._record_index >= self._max_records:
            self._record_index = 0
        record = {
            'time': round(time.time()),  # time in seconds since the epoch
            'location': current_access_point,
            'stats': {
                key: {
                    'readReqs': key_statistics.num_get_reqs,
                    'writeReqs': key_statistics.num_set_reqs,
                    'avgSize': key_statistics.cumulative_value_size / (key_statistics.num_get_reqs
                                                                       + key_statistics.num_set_reqs)
                } for key, key_statistics in statistics.items()
            }
        }
        self._schedule_write_task(record_index, record)

    async def close(self):
        # wait for background tasks to complete
        await asyncio.gather(*self._bg_write_tasks)
        if self._initialized:
            # delete the client's path in the shared JSON document
            await self._remote_json.delete(f'$.{self._client_id}')
