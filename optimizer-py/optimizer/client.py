import asyncio
import time
from dml.asyncio.client import BaseTcpClient
from optimizer.commands import GetStatistic, GetMainObjectLocations, GetUnitPrices, GetBills, ClearStatistics, \
    GetFreeMemory
from dml.asyncio.client import DmlClient
import pandas as pd

from dml.storage.objects import BsonArgsCodec

STATISTIC_SERVER_BASE_PORT = 7000


class StatisticEndpoint:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.verticle_id = port - STATISTIC_SERVER_BASE_PORT

    def __str__(self):
        return f"StatisticEndpoint: {self.host}:{self.port}"


class StatisticClient(BaseTcpClient):
    """
    Statistics Client
    """

    def __init__(self, host, port):
        super().__init__(host, port)
        self.args_codec = BsonArgsCodec()

    async def get_statistics(self):
        return await self._protocol.request(GetStatistic())

    async def clear_statistics(self):
        return await self._protocol.request(ClearStatistics())

    async def get_main_objects_locations(self):
        return await self._protocol.request(GetMainObjectLocations())

    async def get_unit_prices(self):
        return await self._protocol.request(GetUnitPrices(args_codec=self.args_codec))

    async def get_bills(self):
        return await self._protocol.request(GetBills(args_codec=self.args_codec))

    async def get_free_memory(self):
        return await self._protocol.request(GetFreeMemory())


class OptimizerClient:
    """
    Optimizer Client (wrapper for dml and storage statistic clients)
    """

    def __init__(self, metadata_host, metadata_port):
        self.metadata_host = metadata_host
        self.metadata_port = metadata_port
        self._dml_client = DmlClient(metadata_host, metadata_port)
        self._storage_statistic_clients = {}

    def get_dml(self):
        return self._dml_client

    async def _disconnect_storage_statistics(self, storage_statistic_endpoint):
        await self._storage_statistic_clients.get(storage_statistic_endpoint).disconnect()

    async def connect(self):
        await self._dml_client.connect()

    async def disconnect(self):
        # disconnect from storage statistic servers
        disconnect_tasks = [self._disconnect_storage_statistics(storage_statistic_endpoint) for
                            storage_statistic_endpoint in self._storage_statistic_clients.keys()]
        # disconnect dml (metadata server and storage servers if opened)
        disconnect_tasks.append(self._dml_client.disconnect())
        await asyncio.gather(*disconnect_tasks)

    async def _get_or_create_storage_statistic_client(self, storage_statistic_endpoint: StatisticEndpoint):
        client = self._storage_statistic_clients.get(storage_statistic_endpoint)
        if client is None:
            client = StatisticClient(storage_statistic_endpoint.host, storage_statistic_endpoint.port)
            await client.connect()
            self._storage_statistic_clients[storage_statistic_endpoint] = client
            client.on_connection_lost.add_done_callback(
                lambda _: self._storage_statistic_clients.pop(storage_statistic_endpoint, None))
        return client

    async def get_statistics(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.get_statistics()

    async def clear_statistics(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.clear_statistics()

    async def get_main_objects_locations(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.get_main_objects_locations()

    async def get_unit_prices(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.get_unit_prices()

    async def get_bills(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.get_bills()

    async def get_bills_of_all_nodes(self):
        all_statistic_endpoints = await self.get_all_statistic_endpoints()
        tasks = [self.get_bills(endpoint) for endpoint in all_statistic_endpoints.values()]
        bills = await asyncio.gather(*tasks)
        bills_by_endpoint = {}
        for endpoint, bill in zip(all_statistic_endpoints.keys(), bills):
            bills_by_endpoint[endpoint] = bill
        return bills_by_endpoint

    async def get_free_memory_of_node(self, storage_statistic_endpoint: StatisticEndpoint):
        statistic_client = await self._get_or_create_storage_statistic_client(storage_statistic_endpoint)
        return await statistic_client.get_free_memory()

    async def get_all_statistic_endpoints(self):
        membership_view = await self._dml_client.get_membership_view()
        all_statistic_endpoints = {}
        for node_key, node_value in membership_view["nodeMap"].items():
            for verticle in node_value["verticles"]:
                if verticle["type"] == "STORAGE":
                    all_statistic_endpoints[int(verticle["id"])] = StatisticEndpoint(node_value["hostname"], verticle[
                        "id"] + STATISTIC_SERVER_BASE_PORT)
        return all_statistic_endpoints

    async def get_statistics_of_all_nodes(self):
        all_statistic_endpoints = (await self.get_all_statistic_endpoints()).values()
        tasks = [self.get_statistics(endpoint) for endpoint in all_statistic_endpoints]
        return await asyncio.gather(*tasks)

    async def get_statistics_of_endpoints(self, endpoints: list[StatisticEndpoint]):
        tasks = [self.get_statistics(endpoint) for endpoint in endpoints]
        return await asyncio.gather(*tasks)

    async def clear_statistics_of_all_nodes(self):
        all_statistic_endpoints = (await self.get_all_statistic_endpoints()).values()
        tasks = [self.clear_statistics(endpoint) for endpoint in all_statistic_endpoints]
        return await asyncio.gather(*tasks)

    '''
    Reformat the statistics to a pandas dataframe in the format required by the initial SDKV optimizer
    Moreover, if matching_keys is set, the entries will be filtered by the keys in matching_keys!
    This is useful if the optimizer should only optimize a part of the keys (e.g. its main keys)!
    '''

    def get_statistics_in_optimizer_sdkv1_pandas_format(self, statistics, time_range, matching_keys: list = None):
        rows = []
        time_range_end = round(time.time())
        time_range_start = time_range_end - time_range
        for node_stats in statistics:
            verticle_zone = next(iter(node_stats)) ### The zone of the verticle that gathered the statistics
            node_stat = node_stats[verticle_zone]
            for timestamp, node_record in node_stat.items():
                if time_range_start <= (timestamp / 1000.0) <= time_range_end:
                    for key, zone_stats in node_record.items():
                        if matching_keys is None or key in matching_keys:
                            for zone, stats in zone_stats.items():
                                row = {'time': timestamp, 'ap': zone, 'key': key,
                                       'get_accesses': stats['num_requests']['GET'],
                                       'set_accesses': stats['num_requests']['PUT'],
                                       'avg_size': sum(stats['cummulative_read_bytes'].values()) + sum(
                                           stats['cummulative_sent_bytes'].values())}
                                row['avg_size'] = float(row['avg_size']) / (row['get_accesses'] + row['set_accesses'])
                                rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=['ap', 'key', 'get_accesses', 'set_accesses', 'avg_size'])
        return df.groupby(['ap', 'key'], as_index=False).agg({
            'get_accesses': 'sum',
            'set_accesses': 'sum',
            'avg_size': 'mean'
        })

    '''
    Reformat the statistics to a pandas dataframe in the format required by the CELL optimizer
    Moreover, if matching_keys is set, the entries will be filtered by the keys in matching_keys!
    This is useful if the optimizer should only optimize a part of the keys (e.g. its main keys)!
    '''

    def get_statistics_in_optimizer_cell_pandas_format(self, statistics, time_range, matching_keys: list = None):
        rows = []
        time_range_end = round(time.time())
        time_range_start = time_range_end - time_range
        for node_stats in statistics:
            verticle_zone = next(iter(node_stats)) ### The zone of the verticle that gathered the statistics
            node_stat = node_stats[verticle_zone]
            for timestamp, node_record in node_stat.items():
                if time_range_start <= (timestamp / 1000.0) <= time_range_end:
                    for key, zone_stats in node_record.items():
                        if matching_keys is None or key in matching_keys:
                            for zone, stats in zone_stats.items():
                                row = {'time': timestamp, 'ap': zone,
                                       'verticle_zone': verticle_zone, 'key': key,
                                       'get_accesses': stats['num_requests']['GET'],
                                       'set_accesses': stats['num_requests']['PUT'],
                                       'avg_size': sum(stats['cummulative_read_bytes'].values()) + sum(
                                           stats['cummulative_sent_bytes'].values())}
                                row['avg_size'] = float(row['avg_size']) / (row['get_accesses'] + row['set_accesses'])
                                rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=['verticle_zone', 'ap', 'key', 'get_accesses', 'set_accesses', 'avg_size'])
        return df.groupby(['verticle_zone', 'ap', 'key'], as_index=False).agg({
            'get_accesses': 'sum',
            'set_accesses': 'sum',
            'avg_size': 'mean'
        })

    def get_statistics_in_optimizer_drep_pandas_format(self, statistics, time_range):
        df = self.get_statistics_in_optimizer_sdkv1_pandas_format(statistics, time_range, None)
        df['num_requests'] = df['get_accesses'] + df['set_accesses']
        df_drep = df.drop(['get_accesses', 'set_accesses', 'avg_size'], axis='columns')
        return df_drep


async def main():
    optimizer_client = OptimizerClient('localhost', 9000)
    await optimizer_client.connect()
    statistics_1 = await optimizer_client.get_statistics_of_all_nodes()
    print(statistics_1)

    await optimizer_client.clear_statistics_of_all_nodes()

    statistics_1 = await optimizer_client.get_statistics_of_all_nodes()
    print(statistics_1)

    statistics_opt1 = optimizer_client.get_statistics_in_optimizer_sdkv1_pandas_format(statistics_1, 10)
    print(statistics_opt1)

    # statistics_drep = optimizer_client.get_statistics_in_optimizer_drep_pandas_format(statistics_1, 100000)
    # print(statistics_drep)

    # main_objects_locations = await optimizer_client.get_main_objects_locations(StatisticEndpoint("localhost", 9001 + STATISTIC_SERVER_BASE_PORT))

    # print(main_objects_locations)

    # unit_prices = await optimizer_client.get_unit_prices(StatisticEndpoint("localhost", 9001 + STATISTIC_SERVER_BASE_PORT))

    # print(unit_prices)

    # bills = await optimizer_client.get_bills(StatisticEndpoint("localhost", 9001 + STATISTIC_SERVER_BASE_PORT))

    # print(bills)

    # all_bills = await optimizer_client.get_bills_of_all_nodes()

    # print(all_bills)

    free_memory = await optimizer_client.get_free_memory_of_node(
        StatisticEndpoint("localhost", 1101 + STATISTIC_SERVER_BASE_PORT))
    print(f"Free memory: {free_memory}")

    await optimizer_client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
