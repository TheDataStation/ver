import os
from os import listdir
from os.path import isfile, join
from typing import Dict
import csv
import json

from tqdm import tqdm

from dindex_store.discovery_index import DiscoveryIndex
from dindex_store.common import EdgeType


# def load_data(path, file_type, discovery_graph: DiscoveryIndex):
#     # Populate the nodes table with files under the specified path
#
#     if file_type != 'json' and file_type != 'csv':
#         print(f'File type {file_type} currently not supported')
#         return
#
#     for filename in os.listdir(path):
#         filepath = os.path.join(path, filename)
#         if os.path.isfile(filepath):
#             if file_type == 'json':
#                 with open(filepath) as f:
#                     profile = json.load(f)
#                     discovery_graph.add_profile(profile)
#             elif file_type == 'csv':
#                 df = pd.read_csv(
#                     filepath, delimiter=CSV_DELIMITER).to_dict('records')
#                 for profile in df:
#                     discovery_graph.add_profile(profile)
#
#
# def build_minhash(discovery_graph, minhash_perm, threshold=0.5):
#     # Construct the graph (edges) based on minHash signatures of the nodes
#     start_time = time.time()
#     content_index = MinHashLSH(threshold, num_perm=minhash_perm)
#     profiles = discovery_graph.get_minhashes()
#     for profile in profiles:
#         profile['minhash'] = MinHash(
#             num_perm=minhash_perm, hashvalues=np.array(
#                 profile['minhash'].split(',')))
#         content_index.insert(profile['id'], profile['minhash'])
#
#     spent_time = time.time() - start_time
#     print(f'Indexed all minHash signatures: Took {spent_time}')
#
#     for profile in profiles:
#         neighbors = content_index.query(profile['minhash'])
#         for neighbor in neighbors:
#             # TODO: Need to check that they are not from the same source
#             # TODO: Replace with actual attributes
#             discovery_graph.add_undirected_edge(
#                 profile['id'], neighbor,
#                 EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {'similar': 1})
#
#
# def dindex_builder(config: Dict):
#     discovery_graph = DiscoveryIndex(config)
#     load_data(config['data_path'], config['file_type'], discovery_graph)
#     build_minhash(discovery_graph, config['minhash_perm'])
#     return discovery_graph









def main(input_data_path, config: Dict):

    # Create an instance of the discovery index
    dindex = DiscoveryIndex(config)

    # Initialize index
    dindex.initialize(config)

    # Check input data type
    if config["input_data_type"] != "json":
        # TODO
        print("Error: only json profiles supported")
        return

    profile_path = input_data_path + "/json/"
    text_path = input_data_path + "/text/"

    # Read profiles and populate index
    for file_path in os.listdir(profile_path):
        if os.path.isfile(file_path):
            with open(file_path) as f:
                profile = json.load(f)
                # Preprocessing minhash on profile
                if "minhash" in profile and profile["minhash"]:
                    profile["minhash"] = ",".join(map(str, profile["minhash"]))
                # add profile
                dindex.add_profile(profile)

    # Read text files and populate index
    onlyfiles = [f for f in listdir(text_path) if isfile(join(text_path, f))]
    for csv_file_path in tqdm(onlyfiles):
        csv_delimiter = config["TEXT_CSV_DELIMITER"]
        with open(csv_file_path) as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=csv_delimiter)
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                    continue
                profile_id, dbName, path, sourceName, columnName, data = int(row[0]), row[1], row[2], row[3], row[4], row[5]

                dindex.add_text_content(profile_id, dbName, path, sourceName, columnName, data)

    # Create content_similarity edges
    # TODO: this could be done incrementally, every time a new node is added, at a cost in efficiency
    profiles = dindex.get_minhashes()
    content_similarity_index = dindex.get_content_similarity_index()
    for profile in profiles:
        neighbors = content_similarity_index.query(profile['minhash'])
        for neighbor in neighbors:
            # TODO: Need to check that they are not from the same source
            # TODO: Replace with actual attributes
            dindex.add_undirected_edge(
                profile['id'], neighbor,
                EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {'similar': 1})

    return dindex


if __name__ == "__main__":
    print("DIndex Builder")

    import config

    cnf = dict(config)
    dindex = main(cnf)

    # TODO: notification
