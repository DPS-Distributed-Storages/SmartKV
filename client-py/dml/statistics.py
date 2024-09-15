import logging
import re
import subprocess
import time
import uuid
from multiprocessing import TimeoutError as MultiprocessingTimeoutError
from threading import Thread

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import ASYNCHRONOUS
from urllib3.exceptions import TimeoutError as Urllib3TimeoutError

from dml.exceptions import MetadataCommandException, MetadataCommandErrorCode
from dml.storage.objects import SharedJson


class KeyStatistics:
    def __init__(self):
        self.num_get_reqs = 0
        self.num_set_reqs = 0
        self.cumulative_value_size = 0


class IwconfigESSIDProvider:
    @staticmethod
    def get_current_ap():
        """
        Returns the ESSID of the wireless network to which the host is currently connected, or None if no wireless
        network is found. Only the first found ESSID is returned if the host has multiple wireless interfaces.
        This is done by executing the iwconfig command and searching for ESSID in the output.
        """
        cmd = 'iwconfig 2>&1 | grep ESSID:'  # 2>&1 redirects stderr of iwconfig to stdout
        lines = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode('utf-8').split('\n')
        if len(lines) <= 0 or lines[0] == '':
            # no line containing ESSID found
            return None
        # read the ESSID value from the first line
        match = re.search(r'\s+ESSID:"([^"]+)"', lines[0])
        if match is None:
            # the ESSID value is empty
            return None
        return match.group(1)


class AccessPointInfoService(Thread):
    """
    A background thread that periodically retrieves information about the access point to which the host is currently
    connected.
    """

    def __init__(self, ap_info_provider=IwconfigESSIDProvider(), interval=1.0):
        """
        :param ap_info_provider: the implementation that provides information about the access point
        :param interval: the interval in seconds
        """
        Thread.__init__(self, daemon=True)
        self.ap_info_provider = ap_info_provider
        self.interval = interval
        self._access_point = None
        self.start()

    @property
    def current_ap(self):
        return self._access_point

    def run(self):
        while True:
            self._access_point = self.ap_info_provider.get_current_ap()
            time.sleep(self.interval)


class StaticAccessPointInfoService:
    def __init__(self, current_ap):
        self.current_ap = current_ap


class StatisticsWriterInfluxDB:
    def __init__(self, url='http://localhost:8086', username='dml', password='dmldmldml',
                 bucket='test', org='dml'):
        self.bucket = bucket
        self.org = org
        self._influx = InfluxDBClient(url=url, username=username, password=password, org=org)
        self._influx_write_api = self._influx.write_api(write_options=ASYNCHRONOUS)
        self._last_write_futures = []

    def _wait_for_last_write(self):
        """
        Blocks until the last write operation is completed.
        """
        try:
            for future in self._last_write_futures:
                future.get(timeout=15)
        except (InfluxDBError, MultiprocessingTimeoutError, Urllib3TimeoutError):
            logging.exception('Failed to write statistics to InfluxDB')
        self._last_write_futures.clear()

    def write_statistics(self, statistics, current_access_point):
        """
        Asynchronously writes the given statistics to InfluxDB. If a previous write operation is still in progress,
        it will block until it is completed.
        """
        if self._last_write_futures:
            self._wait_for_last_write()
        for key, key_statistics in statistics.items():
            self._last_write_futures.append(self._influx_write_api.write(self.bucket, self.org, {
                'measurement': 'dml',
                'tags': {
                    'key': key,
                    'ap': current_access_point
                },
                'fields': {
                    'get_accesses': key_statistics.num_get_reqs,
                    'set_accesses': key_statistics.num_set_reqs,
                    'avg_size':
                        key_statistics.cumulative_value_size / (key_statistics.num_get_reqs + key_statistics.num_set_reqs)
                }
            }))

    def close(self):
        """
        Waits for all write operations to complete and then closes the writer.
        """
        self._wait_for_last_write()
        self._influx.close()


class StatisticsWriterDml:
    DEFAULT_KEY = '__clientStatistics'

    def __init__(self, dml, client_id=str(uuid.uuid4()), key=DEFAULT_KEY, max_records=6):
        self._dml = dml
        self._client_id = client_id
        self._initialized = False
        self._remote_json = SharedJson(self._dml, key)
        self._record_index = 0
        self._max_records = max_records

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
            self._remote_json.put(str(self._client_id), {})
        except MetadataCommandException as err:
            if err.num == MetadataCommandErrorCode.KEY_DOES_NOT_EXIST.value:
                # the shared JSON document does not exist yet, so we create it
                self._remote_json = SharedJson(self._dml, self._remote_json.key, json={str(self._client_id): {}})
            else:
                raise

    async def _write_statistics(self, record_index, record):
        if not self._initialized:
            await self._init_client_path()
            self._initialized = True
        self._remote_json.put(str(record_index), record, path=f'$.{self._client_id}')

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
        self._write_statistics(record_index, record)

    async def close(self):
        if self._initialized:
            # delete the client's path in the shared JSON document
            self._remote_json.delete(f'$.{self._client_id}')


class Statistics:
    def __init__(self, writer, ap_info_service=AccessPointInfoService(), flush_interval=5000):
        self.ap_info_service = ap_info_service
        self.writer = writer
        self.flush_interval = flush_interval
        self._last_submission = time.time()
        self._stats = {}  # maps keys to key statistics

    def _get_or_create_key_statistics(self, key):
        """
        Returns the statistics collected for the given key or creates a new key statistics object if it does not exist.
        """
        key_stats = self._stats.get(key)
        if key_stats is None:
            key_stats = KeyStatistics()
            self._stats[key] = key_stats
        return key_stats

    def _check_flush(self):
        """
        Calls the flush method if the flush interval is exceeded. Otherwise, it does nothing.
        """
        if (time.time() - self._last_submission) * 1000 >= self.flush_interval:
            self.flush()

    def add_storage_get_req(self, key, value_size):
        """
        Adds the given storage get request to the statistics.
        """
        if key.startswith('__'):
            # ignore internal keys
            return
        key_stats = self._get_or_create_key_statistics(key)
        key_stats.num_get_reqs += 1
        key_stats.cumulative_value_size += value_size
        self._check_flush()

    def add_storage_set_req(self, key, value_size):
        """
        Adds the given storage set request to the statistics.
        """
        if key.startswith('__'):
            # ignore internal keys
            return
        key_stats = self._get_or_create_key_statistics(key)
        key_stats.num_set_reqs += 1
        key_stats.cumulative_value_size += value_size
        self._check_flush()

    def flush(self):
        """
        Submits the collected statistics to the statistics writer, deletes them locally and resets the flush timer.
        """
        self._last_submission = time.time()
        if not self._stats:
            # we do not have any statistics
            return
        access_point = self.ap_info_service.current_ap
        if access_point is None:
            # not connected to an access point
            return
        self.writer.write_statistics(self._stats, access_point)
        self._stats = {}
