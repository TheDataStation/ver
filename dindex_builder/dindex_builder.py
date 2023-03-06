import json
import os
import time
import numpy as np
import pandas as pd

from typing import Dict
from datasketch import MinHash, MinHashLSH

from dindex_store.discovery_index import DiscoveryIndex
from dindex_store.common import EdgeType

CSV_DELIMITER = ','


def load_data(path, file_type, discovery_graph: DiscoveryIndex):
    # Populate the nodes table with files under the specified path

    if file_type != 'json' and file_type != 'csv':
        print(f'File type {file_type} currently not supported')
        return

    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            if file_type == 'json':
                with open(filepath) as f:
                    profile = json.load(f)
                    discovery_graph.add_profile(profile)
            elif file_type == 'csv':
                df = pd.read_csv(
                    filepath, delimiter=CSV_DELIMITER).to_dict('records')
                for profile in df:
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
                profile['id'], neighbor, 
                EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {'similar': 1})

def dindex_builder(config: Dict):
    discovery_graph = DiscoveryIndex(config)
    load_data(config['data_path'], config['file_type'], discovery_graph)
    build_minhash(discovery_graph, config['minhash_perm'])
    return discovery_graph
