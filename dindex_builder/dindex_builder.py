import json
import os
import time
import numpy as np

from typing import Dict
from datasketch import MinHash, MinHashLSH

from dindex_store.discovery_index import DiscoveryIndex
from dindex_store.common import EdgeType


def load_json_data(path, discovery_graph: DiscoveryIndex):
    # Populate the nodes table with JSON files under the specified path
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            with open(filepath) as f:
                profile = json.load(f)
                discovery_graph.add_profile(profile)


def build_minhash(discovery_graph, minhash_perm, threshold=0.5):
    # Construct the graph (edges) based on minHash signatures of the nodes
    start_time = time.time()
    content_index = MinHashLSH(threshold, num_perm=minhash_perm)
    profiles = discovery_graph.get_minhashes()
    for profile in profiles:
        profile['minhash'] = MinHash(
            num_perm=minhash_perm, hashvalues=np.array(
                profile['minhash'].split(',')))
        content_index.insert(profile['id'], profile['minhash'])

    spent_time = time.time() - start_time
    print(f'Indexed all minHash signatures: Took {spent_time}')

    for profile in profiles:
        neighbors = content_index.query(profile['minhash'])
        for neighbor in neighbors:
            # TODO: Need to check that they are not from the same source
            # TODO: Replace with actual attributes
            discovery_graph.add_undirected_edge(
                profile['id'], neighbor, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {
                    "similar": 1})


def dindex_builder(config: Dict):
    discovery_graph = DiscoveryIndex(config)
    load_json_data(config["data_path"], discovery_graph)
    build_minhash(discovery_graph, config["minhash_perm"])
    return discovery_graph
