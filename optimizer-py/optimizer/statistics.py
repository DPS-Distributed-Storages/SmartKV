import time

import pandas as pd

from dml.asyncio.objects import SharedJson
from dml.asyncio.statistics import StatisticsWriterDml


class StatisticsReaderDml:
    def __init__(self, dml, key=StatisticsWriterDml.DEFAULT_KEY):
        self._dml = dml
        self._remote_json = SharedJson(self._dml, key)

    @property
    def dml(self):
        return self._dml

    @dml.setter
    def dml(self, dml):
        self._dml = dml
        self._remote_json.dml = dml

    async def get_sizes(self, time_range):
        raise NotImplementedError()

    async def get_all(self, time_range):
        """Retrieves access statistics of all keys."""
        statistics_dict = await self._remote_json.get()
        time_range_end = round(time.time())
        time_range_start = time_range_end - time_range
        rows = [{'client': client, 'record': record_id, 'time': record['time'], 'ap': record['location'],
                 'key': key, 'get_accesses': stats['readReqs'], 'set_accesses': stats['writeReqs'],
                 'avg_size': stats['avgSize']}
                for client, records in statistics_dict.items()
                for record_id, record in records.items()
                for key, stats in record['stats'].items()
                if time_range_start <= record['time'] <= time_range_end]
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=['ap', 'key', 'get_accesses', 'set_accesses', 'avg_size'])
        return df.groupby(['ap', 'key'], as_index=False).agg({
            'get_accesses': 'sum',
            'set_accesses': 'sum',
            'avg_size': 'mean'
        })

    async def get_traces_key(self, key, time_range):
        raise NotImplementedError()


class StatisticsReaderInfluxDB:
    def __init__(self, influx_client, bucket):
        self.influx = influx_client
        self.bucket = bucket

    async def get_sizes(self, time_range):
        """Retrieves payload sizes of key/value accesses."""
        q = '''
        from(bucket: bucketParam)
            |> range(start: 0)
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> keep(columns: ["_measurement", "key", "avg_size"])
            |> group(columns: ["_field", "key"])
            |> last(column: "key")
        '''
        p = {
            "bucketParam": self.bucket,
            "timeParam": "-" + str(time_range) + "s"
        }
        df_size = await self.influx.query_api().query_data_frame(org="dml", query=q, params=p)
        return df_size

    async def get_all(self, time_range):
        """Retrieves access statistics of all keys."""
        q = '''
        from(bucket: bucketParam)
            |> range(start: duration(v : timeParam))
            |> filter(fn: (r) => r._measurement == "dml")            
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> reduce(
                fn: (r, accumulator) =>
                    ({
                        get_accesses: accumulator.get_accesses + r.get_accesses,
                        set_accesses: accumulator.set_accesses + r.set_accesses,
                        count: accumulator.count + 1.0,
                        total: accumulator.total + r.avg_size,
                        avg_size: float(v: accumulator.total + r.avg_size) / float(v: accumulator.count + 1.0),
                    }),
                identity: {get_accesses: 0, set_accesses: 0, count: 0.0, total: 0.0, avg_size: 0.0},
            )
            |> keep(columns: ["ap", "key", "get_accesses", "set_accesses", "count", "total", "avg_size"])
        '''
        p = {
            "bucketParam": self.bucket,
            "timeParam": "-" + str(time_range) + "s"
        }
        df_traces = await self.influx.query_api().query_data_frame(org="dml", query=q, params=p)
        return df_traces

    async def get_traces_key(self, key, time_range):
        """Retrieves access statistics of a specific key."""
        q = '''
        from(bucket: bucketParam)
            |> range(start: duration(v : timeParam))
            |> filter(fn: (r) => r._measurement == "dml" and r.key == keyParam)
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> reduce(
                fn: (r, accumulator) =>
                    ({
                        get_accesses: accumulator.get_accesses + r.get_accesses,
                        set_accesses: accumulator.set_accesses + r.set_accesses,
                        count: accumulator.count + 1.0,
                        total: accumulator.total + r.avg_size,
                        avg_size: float(v: accumulator.total + r.avg_size) / float(v: accumulator.count + 1.0),
                    }),
                identity: {get_accesses: 0, set_accesses: 0, count: 0.0, total: 0.0, avg_size: 0.0},
            )
            |> keep(columns: ["ap", "key", "get_accesses", "set_accesses", "count", "total", "avg_size"])
        '''
        p = {
            "bucketParam": self.bucket,
            "keyParam": key,
            "timeParam": "-" + str(time_range) + "s"
        }
        df_traces = await self.influx.query_api().query_data_frame(org="dml", query=q, params=p)
        return df_traces
