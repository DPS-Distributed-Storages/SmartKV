import csv
import random
from collections import defaultdict

import networkx as nx
import numpy as np
from matplotlib import pyplot as plt

random.seed(3)


class GraphTopologyReader:
    """Reads the topology from the files nodes.csv and edges.csv."""

    @staticmethod
    def create_graph():
        """Function to create a graph out of the information provided in the topology files."""
        graph = nx.Graph()
        with open("nodes.csv", 'r') as file:
            csvreader = csv.reader(file, delimiter=',', skipinitialspace=True)
            next(csvreader)
            for row in csvreader:
                name, node_type, x_pos, y_pos, host, memory, region, zone = row
                pos = (int(x_pos), int(y_pos))
                graph.add_node(str(name), node_type=node_type, pos=pos, host=host, memory=memory, region=region,
                               zone=zone)
        with open("edges.csv", 'r') as file:
            csvreader = csv.reader(file, delimiter=',', skipinitialspace=True)
            next(csvreader)
            for row in csvreader:
                node1, node2, latency, bw = row
                graph.add_edge(str(node1), str(node2), latency=float(latency), bw=float(bw))
        return graph


class GraphTopology:
    """Stores the topology of the network in a graph-based format."""

    def __init__(self, graph_provider=GraphTopologyReader()):
        self.network_graph = graph_provider.create_graph()
        self.storages = self.get_network_nodes_by_node_type('storage')  # Storage node IDs
        self.aps = self.get_network_nodes_by_node_type('ap')  # Access points
        self.storage_ap_latencies = None
        self.storage_ap_bandwidths = None
        self.storage_latencies = None
        self.storage_bandwidths = None
        self._init_latency_and_bw()

    def get_ap_to_storage_latency(self, from_ap, to_storage):
        return self.storage_ap_latencies[self.storages.index(to_storage)][self.aps.index(from_ap)]

    def get_storage_latency(self, from_storage, to_storage):
        return self.storage_latencies[self.storages.index(from_storage)][self.storages.index(to_storage)]

    def get_node_attribute(self, node, attribute):
        return self.network_graph.nodes[node][attribute]

    def get_network_nodes_by_node_type(self, node_type):
        return [node for node in self.network_graph.nodes if self.network_graph.nodes[node]['node_type'] == node_type]

    def _init_latency_and_bw(self):
        self.storage_ap_latencies = np.zeros((len(self.storages), len(self.aps)))
        self.storage_ap_bandwidths = np.zeros((len(self.storages), len(self.aps)))
        for i, storage in enumerate(self.storages):
            for j, ap in enumerate(self.aps):
                shortest = nx.dijkstra_path(self.network_graph, ap, storage)
                self.storage_ap_latencies[i][j] = nx.path_weight(self.network_graph, shortest, "latency")
                self.storage_ap_bandwidths[i][j] = nx.path_weight(self.network_graph, shortest, "bw")
        self.storage_latencies = np.zeros((len(self.storages), len(self.storages)))
        self.storage_bandwidths = np.zeros((len(self.storages), len(self.storages)))
        for i, storage1 in enumerate(self.storages):
            for j, storage2 in enumerate(self.storages):
                shortest = nx.dijkstra_path(self.network_graph, storage1, storage2)
                self.storage_latencies[i][j] = nx.path_weight(self.network_graph, shortest, "latency")
                self.storage_bandwidths[i][j] = nx.path_weight(self.network_graph, shortest, "bw")

    def plot_graph(self, node_to_optimize=None, save=False):
        """Plot the network graph with edges."""
        plt.figure(1, figsize=(24, 12))
        # plt.subplots()
        plt.xlabel("X-position of storage and ap nodes")
        plt.ylabel("Y-position")
        plt.grid(False)
        # pos = nx.get_node_attributes(self.network_graph, 'pos')
        pos = nx.kamada_kawai_layout(self.network_graph)
        color_map = []
        node_to_optimize = str(node_to_optimize)
        for node in self.network_graph.nodes:
            if node_to_optimize is None or node != node_to_optimize:
                color_map.append('lightgray')
            else:
                color_map.append('orange')

        nx.draw_networkx_nodes(self.network_graph, pos, node_size=500, node_color=color_map)
        nx.draw_networkx_labels(self.network_graph, pos, font_color='black')
        nx.draw_networkx_edges(self.network_graph, pos,
                               edgelist=self.network_graph.edges(), edge_color='blue', width=2, alpha=0.5)
        edge_labels = {(u, v): f"latency: {data['latency']}\nbw: {data['bw']}" for u, v, data in
                       self.network_graph.edges(data=True)}
        nx.draw_networkx_edge_labels(self.network_graph, pos, edge_labels=edge_labels, font_size=6)
        if save:
            plt.savefig("graph.png", dpi=400)
        plt.show()


class ZoneGraphTopology(GraphTopology):

    def __init__(self):
        super().__init__(GraphTopologyReader())
        self.zone_storages = None
        self.zone_clients = None
        self.ap_to_zone = None
        self.closest_storage_zones = None
        self._init_zone_latency()

    def _init_zone_latency(self):
        self.zone_clients = defaultdict(list)
        self.zone_storages = defaultdict(list)
        self.ap_to_zone = defaultdict(str)
        for node in self.get_network_nodes_by_node_type('ap'):
            self.zone_clients[self.get_node_attribute(node, 'zone')].append(node)
            self.ap_to_zone[node] = self.get_node_attribute(node, 'zone')
        for node in self.get_network_nodes_by_node_type('storage'):
            self.zone_storages[self.get_node_attribute(node, 'zone')].append(node)
        self.closest_storage_zones = defaultdict(list)
        for j, (client_zone, clients) in enumerate(self.zone_clients.items()):
            for i, (storage_zone, storages) in enumerate(self.zone_storages.items()):
                shortest = nx.dijkstra_path(self.network_graph, clients[0], storages[0])
                latency = nx.path_weight(self.network_graph, shortest, "latency")
                self.closest_storage_zones[client_zone].append((storage_zone, latency))
            self.closest_storage_zones[client_zone] = sorted(self.closest_storage_zones[client_zone],
                                                             key=lambda x: x[1])


""" 
This topology is used by the D-Rep optimizer to get neighborhood information of storage nodes 
The network mimics the latency from clients to storage nodes in terms of a Drep style network!
"""


class DrepGraphTopology(GraphTopology):

    def __init__(self, verticle_id_to_optimize):
        super().__init__(GraphTopologyReader())
        ### Maps each access point to its closest storage node
        ### This storage node is the first storage node contacted by a client that is connected to the access point
        self.ap_to_closest_storage_node = {}
        self.storage_nodes = set()
        self.clients = set()
        self.latencies = []
        self.previous_storage_node = {}
        ### In Drep, each optimizer instance only optimizes keys in its associated storage node!
        self.storage_node_to_optimize = str(verticle_id_to_optimize)
        self._init_drep_topology()
        self._init_drep_neighborhood_info()

    """ reconnect an edge by changing one node and correct its weight by adding latency_diff to the latency."""

    def _reconnect(self, node1, node_to_remove, new_node, old_data, latency_diff):
        G = self.network_graph
        latency = old_data['latency']
        bw = old_data['bw']
        latency = round(latency + latency_diff, 2)
        G.remove_edge(node1, node_to_remove)
        G.add_edge(str(node1), str(new_node), latency=latency, bw=bw)

    def _init_drep_topology(self):
        G = self.network_graph

        ### Reconnect edges between AP's and storages within a zone to mimic a Drep-like topology
        for ap in self.get_network_nodes_by_node_type('ap'):
            storage_edges = [(ap, storage, data) for _, storage, data in G.edges(ap, data=True) if
                             self.get_node_attribute(storage, 'node_type') == 'storage']
            min_weight_edge = min(storage_edges, key=lambda x: G.get_edge_data(x[0], x[1])["latency"])
            min_latency = G.get_edge_data(min_weight_edge[0], min_weight_edge[1])["latency"]
            closest_ap_storage_node = min_weight_edge[1]
            self.ap_to_closest_storage_node[ap] = closest_ap_storage_node
            for (ap_node, storage_node, data) in storage_edges:
                self.storage_nodes.add(str(storage_node))
                if (ap_node, storage_node, data) != min_weight_edge:
                    self._reconnect(storage_node, ap_node, closest_ap_storage_node, data, -min_latency)

            client_edges = [(ap, client, data) for _, client, data in G.edges(ap, data=True) if
                            self.get_node_attribute(client, 'node_type') == 'client']
            zone_storages = [storage for _, storage, _ in storage_edges]
            for (ap_node, client_node, data) in client_edges:
                self.clients.add(str(client_node))
                connected_storage_node = closest_ap_storage_node
                self._reconnect(client_node, ap_node, connected_storage_node, data, min_latency)

            ### Remove access points:
            (_, region_switch, data) = \
                [(ap, region_switch, data) for _, region_switch, data in G.edges(ap, data=True) if
                 self.get_node_attribute(region_switch, 'node_type') == 'switch'][0]
            self._reconnect(closest_ap_storage_node, ap, region_switch, data, min_latency)
            G.remove_edge(region_switch, ap)
            G.remove_node(ap)

        ### Reconnect edges between storages and the region switch within a region to mimic a Drep-like topology
        for region_switch in self.get_network_nodes_by_node_type('switch'):
            storage_edges = [(region_switch, storage_node, data) for _, storage_node, data in
                             G.edges(region_switch, data=True) if
                             self.get_node_attribute(storage_node, 'node_type') == 'storage']
            if len(storage_edges) == 0:
                continue
            min_weight_edge = min(storage_edges, key=lambda x: G.get_edge_data(x[0], x[1])["latency"])
            min_latency = G.get_edge_data(min_weight_edge[0], min_weight_edge[1])["latency"]
            closest_storage_node = min_weight_edge[1]
            for (region_switch_node, storage_node, data) in storage_edges:
                if (region_switch_node, storage_node, data) != min_weight_edge:
                    self._reconnect(storage_node, region_switch, closest_storage_node, data, -min_latency)

            ### Remove region switches:
            (_, main_switch, data) = \
                [(region_switch, main_switch, data) for _, main_switch, data in G.edges(region_switch, data=True) if
                 self.get_node_attribute(main_switch, 'node_type') == 'switch'][0]
            self._reconnect(closest_storage_node, region_switch, main_switch, data, min_latency)
            G.remove_edge(region_switch, main_switch)
            G.remove_node(region_switch)

        ### Remove main switch
        main_switch = self.get_network_nodes_by_node_type('switch')
        if len(main_switch) != 1:
            print(f"Error! There are {1} main switches but not a single one!")
            exit(0)
        main_switch = main_switch[0]
        main_switch_edges = G.edges(main_switch, data=True)
        connected_nodes = set()
        latency_to_main_switch = {}
        bw_to_main_switch = {}
        for (_, storage_node, data) in main_switch_edges:
            latency_to_main_switch[storage_node] = data['latency']
            bw_to_main_switch[storage_node] = data['bw']
            connected_nodes.add(storage_node)

        for node1 in connected_nodes:
            G.remove_edge(node1, main_switch)
            for node2 in connected_nodes:
                if node1 != node2:
                    G.add_edge(str(node1), str(node2),
                               latency=latency_to_main_switch[node1] + latency_to_main_switch[node2],
                               bw=min(bw_to_main_switch[node1], bw_to_main_switch[node2]))

        G.remove_node(main_switch)

    def _init_drep_neighborhood_info(self):
        G = self.network_graph
        for j, client in enumerate(self.clients):
            shortest = nx.dijkstra_path(self.network_graph, client, self.storage_node_to_optimize)
            if len(shortest) > 2:
                self.previous_storage_node[shortest[1]] = (
                    shortest[-2], G.get_edge_data(shortest[-2], self.storage_node_to_optimize)['latency'])
            else:
                self.previous_storage_node[shortest[1]] = (shortest[-1], 0)


class CellGraphTopology(GraphTopology):
    def __init__(self):
        super().__init__(GraphTopologyReader())
        self.zone_storages = None
        self.storage_to_zone = None
        self.zone_to_region = None
        self.zone_clients = None
        self.ap_to_zone = None
        self.closest_storage_zones = None
        self.zone_latencies = None
        self._init_zone_latency()
        self._central_zone_cache = defaultdict(str)

    def _init_zone_latency(self):
        self.zone_clients = defaultdict(list)
        self.zone_storages = defaultdict(list)
        self.ap_to_zone = defaultdict(str)
        self.zone_latencies = defaultdict(str)
        self.storage_to_zone = defaultdict(int)
        self.zone_to_region = defaultdict(str)

        for node in self.get_network_nodes_by_node_type('ap'):
            self.zone_clients[self.get_node_attribute(node, 'zone')].append(node)
            self.ap_to_zone[node] = self.get_node_attribute(node, 'zone')
            self.zone_to_region[self.get_node_attribute(node, 'zone')] = self.get_node_attribute(node, 'region')
        for node in self.get_network_nodes_by_node_type('storage'):
            self.zone_storages[self.get_node_attribute(node, 'zone')].append(node)
            self.storage_to_zone[int(node)] = self.get_node_attribute(node, 'zone')
        self.closest_storage_zones = defaultdict(list)
        for j, (client_zone, clients) in enumerate(self.zone_clients.items()):
            for i, (storage_zone, storages) in enumerate(self.zone_storages.items()):
                shortest = nx.dijkstra_path(self.network_graph, clients[0], storages[0])
                latency = nx.path_weight(self.network_graph, shortest, "latency")
                self.closest_storage_zones[client_zone].append((storage_zone, latency))
            self.closest_storage_zones[client_zone] = sorted(self.closest_storage_zones[client_zone],
                                                             key=lambda x: x[1])

        for ap1 in self.get_network_nodes_by_node_type('ap'):
            zone1 = self.get_node_attribute(ap1, 'zone')
            self.zone_latencies[zone1] = {}
            for ap2 in self.get_network_nodes_by_node_type('ap'):
                zone2 = self.get_node_attribute(ap2, 'zone')
                if zone1 == zone2:
                    self.zone_latencies[zone1][zone2] = 0
                shortest = nx.dijkstra_path(self.network_graph, ap1, ap2)
                latency = nx.path_weight(self.network_graph, shortest, "latency")
                self.zone_latencies[zone1][zone2] = latency

    def get_zone2zone_latency(self, zone1, zone2):
        return self.zone_latencies[zone1][zone2]

    def _average_latency_to_client_zones_subset(self, source_zone, client_zones_subset):
        total_latency = 0.0
        for zone in client_zones_subset:
            total_latency += self.get_zone2zone_latency(source_zone, zone)
        return total_latency / len(client_zones_subset)

    def central_zone_for_client_zones_subset(self, client_zones_subset):
        client_zones_subset_tuple = tuple(client_zones_subset)
        if client_zones_subset_tuple in self._central_zone_cache:
            return self._central_zone_cache[client_zones_subset_tuple]
        average_latencies = {zone: self._average_latency_to_client_zones_subset(zone, client_zones_subset) for zone in
                             self.zone_storages.keys()}
        central_zone = min(average_latencies, key=average_latencies.get)
        self._central_zone_cache[client_zones_subset_tuple] = central_zone
        return central_zone
