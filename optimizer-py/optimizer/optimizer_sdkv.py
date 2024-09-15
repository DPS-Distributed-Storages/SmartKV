import argparse
import asyncio
import time

import numpy as np

from optimizer.topology import ZoneGraphTopology
from optimizer.client import OptimizerClient, StatisticEndpoint
from optimizer.client import STATISTIC_SERVER_BASE_PORT

class Optimizer:
    """
    Optimizer
    """

    def __init__(self, topology, time_range):
        self.topo = topology
        self.time_range = time_range

    async def optimize_all(self, df_stats, allowed_replicas, current_configurations, reconfig_threshold):
        """
        Optimize placement of all stored keys
        :param df_stats: dataframe containing access statistics
        :param allowed_replicas: strict number of replicas that must be used
        :param current_configurations: current placements of keys
        :return: proposed reconfigurations e.g., {'<key A>': ['Storage A'], '<key B>': ['Storage C']}
        """

        df_stats['total_accesses'] = df_stats['get_accesses'] + df_stats['set_accesses']

        # associate client zones (APs) with the closest zone hosting storages
        df_stats['storage_zone'] = df_stats['ap'].apply(
            lambda x: self.topo.closest_storage_zones[self.topo.ap_to_zone[x]][0][0])

        df_rank = df_stats.groupby(['storage_zone', 'key'], as_index=False).agg({
            'avg_size': 'mean',
            'get_accesses': 'sum',
            'set_accesses': 'sum',
            'total_accesses': 'sum'
        })

        result = {}
        for key in df_rank['key'].unique():
            proposed_storages = []
            rank_idx = 0
            df_rank_key = df_rank[df_rank['key'] == key]

            while len(proposed_storages) < allowed_replicas and rank_idx < len(df_rank_key.index):
                storage_zone = df_rank_key.iloc[rank_idx]['storage_zone']
                storage_node = self.topo.zone_storages[storage_zone][0]
                proposed_storages.append(storage_node)
                rank_idx += 1

            # Check if we need additional replicas
            if allowed_replicas > len(proposed_storages) > 0:
                # Get all storages and order them by the distance to the first proposed storage
                alternative_rank = []
                for zone in self.topo.closest_storage_zones[self.topo.get_node_attribute(proposed_storages[0], 'zone')]:
                    storages_in_zone = self.topo.zone_storages[zone[0]]
                    for storage in storages_in_zone:
                        if storage not in proposed_storages:
                            alternative_rank.append(storage)
                # Add storages to the proposed storages until we have enough. Since the alternative rank is ordered by
                # distance, we favor storages close to the first proposed storage.
                rank_idx = 0
                while len(proposed_storages) < allowed_replicas and rank_idx < len(alternative_rank):
                    proposed_storages.append(alternative_rank[rank_idx])
                    rank_idx += 1

            # Sort storages by the expected write accesses (write accesses always go to the first storage)
            proposed_storages = self.sort_storages_by_writes(df_stats, key, proposed_storages)
            # Check whether a reconfiguration is worthwhile
            current_storages = [str(storage.id) for storage in current_configurations[key].replicas]
            score_diff = self.compute_placement_score_diff(df_stats, key, current_storages, proposed_storages)
            proposed_storages = proposed_storages if abs(score_diff) > reconfig_threshold else current_storages
            result[key] = proposed_storages

        return result

    def compute_placement_score_diff(self, df_stats, key, current_storage_ids, new_storage_ids):
        if new_storage_ids == current_storage_ids:
            return 0.0
        # Note that a lower score (RTT) is better
        old_score = self.average_rtt(df_stats, key, current_storage_ids)
        new_score = self.average_rtt(df_stats, key, new_storage_ids)
        rel_score_diff = (old_score - new_score) / old_score if old_score != 0.0 else 0.0
        return rel_score_diff

    def sort_storages_by_writes(self, df_stats, key, storages):
        """Sorts the given storages by the expected write accesses to the given key in descending order."""
        set_accesses = []
        for storage in storages:
            # Get the closest AP to the storage
            closest_ap_idx = np.argmin(self.topo.storage_ap_latencies[self.topo.storages.index(storage)])
            closest_ap = self.topo.aps[closest_ap_idx]
            # Get the number of write accesses to the key from the closest AP
            num_set_accesses = \
                df_stats[(df_stats['ap'] == closest_ap) & (df_stats['key'] == key)]['set_accesses'].sum()
            set_accesses.append(num_set_accesses)
        return [storage for _, storage in sorted(zip(set_accesses, storages), reverse=True)]

    def rtt_for_read_req(self, ap, replicas):
        return min([2 * self.topo.get_ap_to_storage_latency(ap, replica) for replica in replicas])

    def rtt_for_write_req(self, ap, replicas):
        replication_rtt = 0 if len(replicas) <= 1 \
            else max([2 * self.topo.get_storage_latency(replicas[0], replica) for replica in replicas[1:]])
        return 2 * self.topo.get_ap_to_storage_latency(ap, replicas[0]) + replication_rtt

    def average_rtt(self, df_stats, key, replicas):
        df_key = df_stats[df_stats['key'] == key]
        sum_requests = 0
        sum_rtts = 0
        for ap in df_key['ap']:
            read_requests = df_key[(df_key['ap'] == ap)]['get_accesses'].sum()
            write_requests = df_key[(df_key['ap'] == ap)]['set_accesses'].sum()
            sum_requests += read_requests + write_requests
            sum_rtts += read_requests * self.rtt_for_read_req(ap, replicas) \
                        + write_requests * self.rtt_for_write_req(ap, replicas)
        return sum_rtts / sum_requests


async def reconfigure_key(dml, key, storage_ids):
    try:
        lock_token = await dml.lock(key)
        await dml.reconfigure(key, storage_ids)
        await dml.unlock(key, lock_token)
    except Exception as e:
        print(e)


async def main(argv=None):
    args = parse_args(argv)

    optimizer_client = OptimizerClient(args.metadata_hostname, args.metadata_port)
    dml = optimizer_client.get_dml()

    #  Create graph and optimizer
    topology = ZoneGraphTopology()
    topology.plot_graph()
    optimizer = Optimizer(topology, args.time_window_length)
    await optimizer_client.connect()

    await asyncio.sleep(args.initial_delay)

    optimizer_verticle_ID = args.optimizer_verticle_ID
    optimizer_verticle_host = args.optimizer_verticle_host

    # Default, all keys will be optimized
    keys_to_optimize = None

    i = 0
    while True:
        start = time.time()
        i += 1
        print(f"Optimization round {i}")
        str_verticles_to_opt = "all verticles" if optimizer_verticle_ID == -1 else "verticle " + str(
            optimizer_verticle_ID)
        print(f"Optimizing for: {str_verticles_to_opt}")
        if (optimizer_verticle_ID == -1):
            statistics = await optimizer_client.get_statistics_of_all_nodes()
        else:
            statistic_endpoint = StatisticEndpoint(optimizer_verticle_host,
                                                   optimizer_verticle_ID + STATISTIC_SERVER_BASE_PORT)
            statistics = await optimizer_client.get_statistics(statistic_endpoint)
            statistics = [statistics]
            keys_to_optimize = (await optimizer_client.get_main_objects_locations(statistic_endpoint)).keys()
        df_stats = optimizer_client.get_statistics_in_optimizer_sdkv1_pandas_format(statistics, args.time_window_length,
                                                                                    keys_to_optimize)
        print('read statistics')
        if df_stats.empty:
            print('no statistics -> nothing to optimize!')
            end = time.time()
            print(f'Optimization took {(end - start)} seconds')
            await asyncio.sleep(args.delay)
            continue
        key_configurations = await dml.get_all_configurations()
        result = await optimizer.optimize_all(df_stats, args.num_replicas,
                                              key_configurations, args.reconfig_threshold)
        reconfig_tasks = []
        for key in result:
            new_storage_ids = result[key]
            current_configuration = key_configurations[key]
            if current_configuration is None:
                continue
            current_storage_ids = [str(storage.id) for storage in current_configuration.replicas]
            if new_storage_ids != current_storage_ids:
                print(time.monotonic(), i, key, 'reconfigure',
                      '"' + ','.join(current_storage_ids) + '"', '"' + ','.join(new_storage_ids) + '"',
                      sep=',')
                reconfig_tasks.append(reconfigure_key(dml, key, [int(storage_id) for storage_id in new_storage_ids]))
        await asyncio.gather(*reconfig_tasks, return_exceptions=True)
        end = time.time()
        print(f'Optimization took {(end - start)} seconds')
        print(f'reconfigured {len(reconfig_tasks)} keys in total!')
        await asyncio.sleep(args.delay)


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='metadata_hostname', default='localhost',
                        help='hostname of a metadata server')
    parser.add_argument('--port', dest='metadata_port', type=int, default=9000,
                        help='port of the metadata server')
    parser.add_argument('--verticleID', dest='optimizer_verticle_ID', type=int, default=-1,
                        help='the ID of the storage verticle that runs this optimizer')
    parser.add_argument('--verticleHost', dest='optimizer_verticle_host', type=str, default='localhost',
                        help='the hostname of the storage verticle that runs this optimizer')
    parser.add_argument('--replicas', dest='num_replicas', type=int, default=1,
                        help='number of replicas per key')
    parser.add_argument('--initial-delay', dest='initial_delay', type=int, default=1,
                        help='delay in seconds to wait before the first execution of the optimizer')
    parser.add_argument('--delay', dest='delay', type=int, default=10,
                        help='delay in seconds to wait between executions of the optimizer')
    parser.add_argument('--window', dest='time_window_length', type=int, default=100,
                        help='the length of the time window in seconds for which to query past client traces')
    parser.add_argument('--reconfig-threshold', dest='reconfig_threshold', type=float, default=0.05)
    return parser.parse_args(args)


if __name__ == '__main__':
    asyncio.run(main())
