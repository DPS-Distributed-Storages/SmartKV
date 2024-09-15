import argparse
import asyncio
import random
import time

import numpy as np

from optimizer.topology import CellGraphTopology
from optimizer.client import OptimizerClient, StatisticEndpoint
from optimizer.client import STATISTIC_SERVER_BASE_PORT

from itertools import combinations


class Optimizer:
    """
    Optimizer
    """

    def __init__(self, topology, time_range, optimizer_client, is_strong_consistency):
        self.topo = topology
        self.time_range = time_range
        self.optimizer_client = optimizer_client
        self.dml = optimizer_client.get_dml()
        self.free_zone_memory = {}
        self.zone_prices = {}
        self.requestUnits = 100.0  #Granularity at which requests are billed (e.g. 1$ per 100 PUT requests)
        self.max_candidates = 3
        self.is_strong_consistency = is_strong_consistency

    async def optimize_all(self, df_stats, current_configurations, _lambda=0.5):
        """
        Optimize placement of all stored keys
        :param df_stats: dataframe containing access statistics
        :param current_configurations: current placements of keys
        :param _lambda: parameter lambda
        :return: results of the performed reconfigurations
        """

        reconfig_tasks = []

        df_stats['total_accesses'] = df_stats['get_accesses'] + df_stats['set_accesses']

        # associate client zones (APs) with the closest zone hosting storages
        df_stats['client_zone'] = df_stats['ap'].apply(
            lambda x: self.topo.closest_storage_zones[self.topo.ap_to_zone[x]][0][0])

        # calculate rank based on total_accesses * (latency from verticle_zone to client zone)
        df_stats['rank'] = df_stats.apply(lambda row: self.calculate_rank(row), axis=1)

        df_rank = df_stats.groupby(['client_zone', 'key'], as_index=False).agg({
            'rank': 'sum',
            'total_accesses': 'sum',
            'avg_size': 'mean',
            'get_accesses': 'sum',
            'set_accesses': 'sum'
        })

        df_rank = df_rank.sort_values(by=['rank'], ascending=False)

        df_rank_agg = df_rank.groupby(['key'], as_index=False).agg({
            'rank': 'sum',
            'avg_size': 'mean',
        })

        df_rank_agg = df_rank_agg.sort_values(by=['rank'], ascending=False)

        ### Reset the zone info before each optimizer run to gather fresh data!
        self.free_zone_memory.clear()
        self.zone_prices.clear()

        ### Retrieve zone costs and free memory asynchronously
        tasks = []
        for zone in df_rank['client_zone'].unique():
            tasks.append(self.gather_zone_info(zone))
        await asyncio.gather(*tasks, return_exceptions=True)

        #result = {}
        for key in df_rank_agg['key'].unique():
            #print('\n')
            current_configuration = current_configurations[key]
            current_storage_zones = self.get_current_replica_zones(current_configuration)
            df_rank_key = df_rank[df_rank['key'] == key]
            df_stats_key = df_stats[df_stats['key'] == key]
            df_rank_key_agg = df_rank_agg[df_rank_agg['key'] == key]
            avg_object_size = int(df_rank_key_agg['avg_size'].iloc[0])
            client_zones = df_rank_key['client_zone'].unique()

            # Calculate center zone, which is the zone with the lowest average latency to all client zones of the current key
            central_zone = self.topo.central_zone_for_client_zones_subset(client_zones)

            # Check if we already gathered pricing and memory info from all replicas and center zone
            # If not, gather it
            await self.check_and_gather_zone_info(current_storage_zones + [central_zone])

            # Calculate latency and costs under the current configuration
            # Performance-wise it is more efficient to do self.average_rtt() and self.total_costs() in one function!
            current_rtt, current_cost = self.total_rtt_and_total_costs(df_stats_key, current_storage_zones, avg_object_size)
            old_utility = current_rtt * _lambda + current_cost
            new_replica_candidates = set()
            new_replica_zone_configurations = []
            new_replica_zone_configuration_benefits = {}

            # Check if removing a replica is beneficial
            if len(current_storage_zones) > 1:
                new_replica_zone_configurations.extend(
                    [current_storage_zones[:i] + current_storage_zones[i + 1:] for i in
                     range(len(current_storage_zones))])

            # Select the most promising zones for replica creation
            for zone in [central_zone] + list(client_zones):
                # Skips a zone if it already holds a replica or if it has no memory left
                if zone not in current_storage_zones and self.free_zone_memory[zone] > 2 * avg_object_size:
                    new_replica_candidates.add(zone)
                if len(new_replica_candidates) >= self.max_candidates:
                    break

            # If there is currently only one replica, check for migrations
            if len(current_storage_zones) == 1:
                new_replica_zone_configurations.extend([[candidate] for candidate in new_replica_candidates])

            # Check for replica duplications
            # Take all possible combinations of adding 1 to self.max_candidates replicas to the current configuration
            additional_replica_combinations = [list(comb) for r in range(1, len(new_replica_candidates) + 1) for comb in
                                               combinations(new_replica_candidates, r)]
            new_replica_zone_configurations.extend(
                [current_storage_zones + comb for comb in additional_replica_combinations])

            # Calculate benefit of all new configuration candidates
            for idx, new_replica_zones in enumerate(new_replica_zone_configurations):
                benefit = self.check_replica_configuration_benefit(df_rank_key, df_stats_key, old_utility,
                                                                   current_storage_zones, new_replica_zones,
                                                                   avg_object_size, _lambda)
                if benefit > 0:
                    new_replica_zone_configuration_benefits[idx] = benefit

            # Select the best proposal for new replica configurations if any was found
            best_proposal = None
            best_proposal_benefit = 0
            # Select best configuration as final proposal
            if len(new_replica_zone_configuration_benefits) != 0:
                best_duplication_idx, best_proposal_benefit = max(new_replica_zone_configuration_benefits.items(),
                                                                  key=lambda x: x[1])
                best_proposal = new_replica_zone_configurations[best_duplication_idx]

            #print(f'Found best proposal {best_proposal} with benefit {best_proposal_benefit}')
            #result[key] = (avg_object_size, best_proposal)

            # Reconfigure keys one after another asynchronously!
            # So in parallel to the remaining optimization.

            reconfig_tasks.append(
               self.check_and_reconfigure(key, current_configuration, current_storage_zones, best_proposal,
                                           avg_object_size))

        result = await asyncio.gather(*reconfig_tasks, return_exceptions=True)

        return [r for r in result if r is True]

    async def check_and_gather_zone_info(self, current_storage_zones):
        tasks = []
        for zone in set(current_storage_zones):
            if not self.zone_prices.__contains__(zone) or not self.free_zone_memory.__contains__(zone):
                tasks.append(self.gather_zone_info(zone))
        await asyncio.gather(*tasks, return_exceptions=True)

    def check_replica_configuration_benefit(self, df_rank_key, df_stats_key, old_utility, current_zones,
                                            new_replica_zones, avg_size, _lambda):
        # Sort the zones by write requests to find the best candidate for the new main node
        new_replica_zones = self.sort_zones_by_writes(df_rank_key, new_replica_zones)

        # Calculate expected rtt and costs under new configuration
        new_rtt, new_cost = self.total_rtt_and_total_costs(df_stats_key, new_replica_zones, avg_size)

        # Calculate rtt and costs to perform the migration
        migration_rtt, migration_cost = self.key_migration_rtt_and_costs(current_zones, new_replica_zones, avg_size)

        # Calculate new utility based on expected rtt and costs plus rtt and costs for migration
        new_utility = (new_rtt + migration_rtt) * _lambda + new_cost + migration_cost

        return old_utility - new_utility

    def get_current_replica_zones(self, key_replicas):
        current_zones = [self.topo.storage_to_zone[storage] for storage in key_replicas]
        return current_zones

    def calculate_rank(self, row):
        client_zone = row['client_zone']
        verticle_zone = row['verticle_zone']
        latency = self.topo.zone_latencies[client_zone][verticle_zone]
        return row['total_accesses'] * latency

    def compute_placement_score_diff(self, df_stats_key, current_storage_ids, new_storage_ids):
        if new_storage_ids == current_storage_ids:
            return 0.0
        # Note that a lower score (RTT) is better
        old_score = self.average_rtt(df_stats_key, current_storage_ids)
        new_score = self.average_rtt(df_stats_key, new_storage_ids)
        rel_score_diff = (old_score - new_score) / old_score if old_score != 0.0 else 0.0
        return rel_score_diff

    def sort_zones_by_writes(self, df_rank_key, replica_zones):
        """Sorts the given zones by the expected write accesses to the given key in descending order."""
        set_accesses = {zone: 0 for zone in replica_zones}
        for client_zone in df_rank_key['client_zone'].unique():
            # Get the closest replica zone for the client zone
            closest_replica_zone = self.get_closest_replica_zone(client_zone, replica_zones)
            # Get the number of write accesses from the client_zone to the key
            num_set_accesses = \
                df_rank_key[df_rank_key['client_zone'] == client_zone]['set_accesses'].sum()
            set_accesses[closest_replica_zone] += num_set_accesses
        return sorted(replica_zones, key=lambda zone: set_accesses[zone], reverse=True)

    def rtt_for_read_req(self, client_zone, replica_zones):
        return min([2 * self.topo.get_zone2zone_latency(client_zone, replica_zone) for replica_zone in replica_zones])

    def rtt_for_write_req(self, client_zone, replica_zones):
        if self.is_strong_consistency:
            main_node_zone = replica_zones[0]
            replication_rtt = 0 if len(replica_zones) <= 1 \
                else 2 * max(
                [self.topo.get_zone2zone_latency(main_node_zone, replica_zone) for replica_zone in replica_zones[1:]])
            return 2 * self.topo.get_zone2zone_latency(client_zone, main_node_zone) + replication_rtt
        else:
            return 2 * max([self.topo.get_zone2zone_latency(client_zone, replica_zone) for replica_zone in replica_zones])


    def key_migration_rtt_and_costs(self, current_zones, new_replica_zones, avg_size):
        current_main_zone = current_zones[0]
        added_zones = [zone for zone in new_replica_zones if zone not in current_zones]
        removed_zones = [zone for zone in current_zones if zone not in new_replica_zones]
        changed_zones = added_zones + removed_zones

        # RTT is calculated as RTT to lock the key on all current replica nodes (initiated by main node's zone)
        # plus the maximum RTT over all PUT/DELETE operations from the current main zone to added/removed zones
        # plus the RTT to again unlock the key on all (new) replica nodes/zones
        migration_rtt = self.rtt_for_write_req(current_main_zone, current_zones) # LOCK
        migration_rtt += self.rtt_for_write_req(current_main_zone, [current_main_zone] + changed_zones) # UPDATE
        migration_rtt += self.rtt_for_write_req(new_replica_zones[0], new_replica_zones) # UNLOCK


        # For the migration we assume 1 GET from the current main zone and
        # 1 PUT/DELETE request that is replicated to all newly added/removed zones
        # We assume no charge for lock and unlock
        migration_costs = self.request_and_transfer_costs(1, 1, avg_size, current_main_zone,
                                                          [current_main_zone] + changed_zones)

        return migration_rtt, migration_costs / 1000.0

    def total_rtt_and_total_costs(self, df_stats_key, replica_zones, avg_key_size):
        sum_rtts = 0
        sum_costs = 0
        for client_zone in df_stats_key['client_zone']:
            client_data = df_stats_key[df_stats_key['client_zone'] == client_zone]
            read_requests = client_data['get_accesses'].sum()
            write_requests = client_data['set_accesses'].sum()
            avg_transfer_size = client_data['avg_size'].mean()
            sum_rtts += read_requests * self.rtt_for_read_req(client_zone, replica_zones) \
                        + write_requests * self.rtt_for_write_req(client_zone, replica_zones)
            sum_costs += self.request_and_transfer_costs(read_requests, write_requests, avg_transfer_size, client_zone,
                                                         replica_zones)
        sum_costs += self.storage_costs(avg_key_size, replica_zones)
        return sum_rtts, sum_costs / 1000.0

    def average_rtt(self, df_stats_key, replica_zones):
        sum_requests = 0
        sum_rtts = 0
        for client_zone in df_stats_key['client_zone']:
            client_data = df_stats_key[df_stats_key['client_zone'] == client_zone]
            read_requests = client_data['get_accesses'].sum()
            write_requests = client_data['set_accesses'].sum()
            sum_requests += read_requests + write_requests
            sum_rtts += read_requests * self.rtt_for_read_req(client_zone, replica_zones) \
                        + write_requests * self.rtt_for_write_req(client_zone, replica_zones)
        return sum_rtts / sum_requests

    # We switch to KB conversion as costs get very small in our experiments with GB conversion
    def bytesCostToKiloBytesCost(self, bytesCost: float):
        return bytesCost / 1024.0

    def total_costs(self, df_stats_key, replica_zones):
        sum_costs = 0
        avg_key_size = 0.0

        for client_zone in df_stats_key['client_zone']:
            read_requests = df_stats_key[(df_stats_key['client_zone'] == client_zone)]['get_accesses'].sum()
            write_requests = df_stats_key[(df_stats_key['client_zone'] == client_zone)]['set_accesses'].sum()
            avg_transfer_size = df_stats_key[(df_stats_key['client_zone'] == client_zone)]['avg_size'].mean()
            avg_key_size += avg_transfer_size
            sum_costs += self.request_and_transfer_costs(read_requests, write_requests, avg_transfer_size, client_zone,
                                                         replica_zones)
        avg_key_size /= len(df_stats_key['client_zone'])
        sum_costs += self.storage_costs(avg_key_size, replica_zones)
        return sum_costs

    def storage_costs(self, avg_size, replica_zones):
        storage_costs = 0.0
        for replica_zone in replica_zones:
            p_s = float(self.zone_prices[replica_zone]["unitStoragePrice"])
            storage_costs += p_s
        return self.bytesCostToKiloBytesCost(storage_costs * self.time_range * avg_size)

    # For performance reasons, we calculate the request and transfer costs in the same functions!
    def request_and_transfer_costs(self, read_requests, write_requests, avg_transfer_size, client_zone, replica_zones):
        request_costs = (
                self.read_request_and_transfer_costs(read_requests, avg_transfer_size, client_zone, replica_zones)
                + self.write_request_and_transfer_costs(write_requests, avg_transfer_size, replica_zones))
        return request_costs

    def read_request_and_transfer_costs(self, read_requests, avg_transfer_size, client_zone, replica_zones):
        closest_replica_zone = self.get_closest_replica_zone(client_zone, replica_zones)

        p_rr = float(self.zone_prices[closest_replica_zone]["unitRequestPriceGET"])
        read_request_costs = read_requests * p_rr

        p_b = self.get_transfer_price(closest_replica_zone, client_zone)
        read_transfer_costs = read_requests * avg_transfer_size * p_b

        return read_request_costs / self.requestUnits + self.bytesCostToKiloBytesCost(read_transfer_costs)

    def write_request_and_transfer_costs(self, write_requests, avg_transfer_size, replica_zones):
        write_request_costs = 0
        write_transfer_costs = 0
        main_zone = replica_zones[0]
        # PUT requests are replicated to every node!
        for idx, replica_zone in enumerate(replica_zones):
            p_wr = float(self.zone_prices[replica_zone]['unitRequestPricePUT'])
            write_request_costs += p_wr

            # We assume that the request from the main node to the client is of size 0! (It is only a status code and thus neglectable).
            # Therefore, we also do not charge this as egress transfer.
            # Note that we also do not charge the egress transfer for any simple status replies, e.g. COMMIT.
            if idx != 0:
                p_b = self.get_transfer_price(main_zone, replica_zone)
                write_transfer_costs += p_b

        return write_requests * write_request_costs / self.requestUnits + self.bytesCostToKiloBytesCost(
            write_requests * avg_transfer_size * write_transfer_costs)

    def get_transfer_price(self, zone1, zone2):
        region1 = self.topo.zone_to_region[zone1]
        region2 = self.topo.zone_to_region[zone2]
        if region1 == region2:
            return float(self.zone_prices[zone1]['unitEgressTransferPrices']['unitTransferPrice']['SAME_REGION'])
        else:
            return float(self.zone_prices[zone1]['unitEgressTransferPrices']['unitTransferPrice']['INTERNET'])

    def get_closest_replica_zone(self, client_zone, replica_zones):
        closest_replica_zone = None
        lowest_latency = float('inf')
        for replica_zone in replica_zones:
            latency = self.topo.get_zone2zone_latency(client_zone, replica_zone)
            if latency < lowest_latency:
                lowest_latency = latency
                closest_replica_zone = replica_zone
        return closest_replica_zone

    async def gather_zone_info(self, zone):
        zone_info = await self.dml.get_zone_info(zone)
        self.free_zone_memory[zone] = int(zone_info["totalFreeMemory"])
        self.zone_prices[zone] = zone_info["weightedAverageUnitPrices"]

    async def check_and_reconfigure(self, key, current_configuration, current_storage_zones, proposed_storage_zones,
                                    object_size):
        if current_configuration is None:
            return False

        if proposed_storage_zones != current_storage_zones and proposed_storage_zones != None:
            proposed_storage_ids = [-1] * len(proposed_storage_zones)
            new_storage_zones = [zone for zone in proposed_storage_zones if zone not in current_storage_zones]
            new_storage_ids = await self.dml.get_free_storage_nodes(new_storage_zones, object_size)
            if -1 in new_storage_ids:
                return False

            new_verticles_count = 0
            for idx, zone in enumerate(proposed_storage_zones):
                # If the zone is already present at the current configuration,
                # we leave the object on the same storage node in that zone
                if zone in current_storage_zones:
                    verticle_id = current_configuration[current_storage_zones.index(zone)]
                    proposed_storage_ids[idx] = verticle_id
                # For new zones, we ask the Metadata Server which node to choose
                else:
                    proposed_storage_ids[idx] = new_storage_ids[new_verticles_count]
                    self.free_zone_memory[zone] -= object_size
                    new_verticles_count += 1
            print(time.monotonic(), key, 'reconfigure',
                  '"' + ','.join(current_storage_zones) + '"', '"' + ','.join(proposed_storage_zones) + '"',
                  '"' + ','.join([str(id) for id in current_configuration]) + '"',
                  '"' + ','.join([str(id) for id in proposed_storage_ids]) + '"',
                  sep=',', flush=True)

            return await reconfigure_key(self.dml, key, [int(storage_id) for storage_id in proposed_storage_ids])


async def reconfigure_key(dml, key, storage_ids):
    try:
        lock_token = await dml.lock(key)
        await dml.reconfigure(key, storage_ids)
        await dml.unlock(key, lock_token)
        return True
    except Exception as e:
        print(e)
        return False

async def main(argv=None):
    args = parse_args(argv)

    optimizer_client = OptimizerClient(args.metadata_hostname, args.metadata_port)
    dml = optimizer_client.get_dml()

    #  Create graph and optimizer
    topology = CellGraphTopology()
    #topology.plot_graph()
    optimizer = Optimizer(topology, args.delay * 1000.0, optimizer_client, args.consistency == 'strong')
    await optimizer_client.connect()

    await asyncio.sleep(args.initial_delay)

    optimizer_verticle_ID = args.optimizer_verticle_ID
    optimizer_verticle_host = args.optimizer_verticle_host

    if optimizer_verticle_ID == -1:
        print("Error! The storage node verticle id that shall be optimized must be specified!")
        return

    i = 0
    while True:
        start = time.time()
        i += 1
        print(f"Optimization round {i}")
        print(f"Optimizing for verticle: {str(optimizer_verticle_ID)} on host: {str(optimizer_verticle_host)}")

        statistic_endpoint = StatisticEndpoint(optimizer_verticle_host,
                                               optimizer_verticle_ID + STATISTIC_SERVER_BASE_PORT)

        main_object_locations = await optimizer_client.get_main_objects_locations(statistic_endpoint)
        keys_to_optimize = main_object_locations.keys()
        all_statistic_endpoints = await optimizer_client.get_all_statistic_endpoints()
        required_verticles = set(value for sublist in main_object_locations.values() for value in sublist)
        required_statistic_endpoints = [statistic_endpoint]
        for verticle in required_verticles:
            required_statistic_endpoints.append(all_statistic_endpoints[verticle])
        statistics = await optimizer_client.get_statistics_of_endpoints(required_statistic_endpoints)
        df_stats = optimizer_client.get_statistics_in_optimizer_cell_pandas_format(statistics, args.delay,
                                                                                   keys_to_optimize)
        print('read statistics')

        if df_stats.empty:
            print('no statistics -> nothing to optimize!')
            end = time.time()
            print(f'Optimization took {(end - start)} seconds')
            await asyncio.sleep(args.delay)
            continue

        result = await optimizer.optimize_all(df_stats, main_object_locations, args._lambda)

        print(f'reconfigured {len(result)} keys in total!')
        end = time.time()
        print(f'Optimization took {(end - start)} seconds')
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
    parser.add_argument('--initial-delay', dest='initial_delay', type=int, default=1,
                        help='delay in seconds to wait before the first execution of the optimizer')
    parser.add_argument('--delay', dest='delay', type=int, default=10,
                        help='delay in seconds to wait between executions of the optimizer')
    parser.add_argument('--reconfig-threshold', dest='reconfig_threshold', type=float, default=0.05)
    parser.add_argument('--lambda', dest='_lambda', type=float,
                        default=0.01)  ### Selected in range [0.01, 0.20] in Drep's evaluation
    parser.add_argument('--consistency', dest='consistency', default='strong')
    return parser.parse_args(args)


if __name__ == '__main__':
    asyncio.run(main())
