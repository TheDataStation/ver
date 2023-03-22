import os
from typing import Dict
import csv
import json

from tqdm import tqdm
import pandas as pd

from dindex_store.discovery_index import DiscoveryIndex
from dindex_store.common import EdgeType


def load_dindex(config: Dict):
    dindex = DiscoveryIndex(config, load=True)
    return dindex


def build_dindex(profile_data_path, config: Dict, force: bool):
    print(f"Building DIndex. profile_data_path: {profile_data_path}")

    # Create an instance of the discovery index
    dindex = DiscoveryIndex(config, force=force)

    # Check input data type
    if config["input_data_type"] != "json":
        # TODO
        print("Error: only json profiles supported")
        return

    profile_path = profile_data_path + "/json/"
    text_path = profile_data_path + "/text/"

    # Read profiles and populate the Profile index
    for file_path in os.listdir(profile_path):
        file_path = os.path.join(profile_path, file_path)
        if os.path.isfile(file_path):
            with open(file_path) as f:
                profile = json.load(f)
                # Preprocessing minhash on profile
                if "minhash" in profile and profile["minhash"]:
                    profile["minhash"] = ",".join(map(str, profile["minhash"]))
                # add profile
                dindex.add_profile(profile)

    # Read text files and populate index
    for csv_file_path in tqdm(os.listdir(text_path)):
        csv_file_path = os.path.join(text_path, csv_file_path)
        if not os.path.isfile(csv_file_path):
            continue
        csv_delimiter = config["text_csv_delimiter"]

        df = pd.read_csv(csv_file_path, names=['profile_id', 'dbName', 'path', 'sourceName',
                             'columnName', 'data'], sep=csv_delimiter)
        for _, row in df.iterrows():
            dindex.add_text_content(row['profile_id'], row['dbName'], row['path'],
                                    row['sourceName'], row['columnName'], row['data'])

    # Create content_similarity edges
    # TODO: this could be done incrementally, every time a new node is added, at a cost in efficiency
    profiles = dindex.get_minhashes()
    content_similarity_index = dindex.get_content_similarity_index()
    for profile in tqdm(profiles):
        neighbors = content_similarity_index.query(profile['minhash'])
        for neighbor in neighbors:
            # TODO: Need to check that they are not from the same source
            # TODO: Replace with actual attributes
            dindex.add_undirected_edge(
                profile['id'], neighbor,
                EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {'similar': 1})
    print("Done building")

    return dindex


if __name__ == "__main__":
    print("DIndex Builder")

    import config

    import argparse

    cnf = {setting: getattr(config, setting) for setting in dir(config)
           if setting.islower() and len(setting) > 2 and setting[:2] != "__"}

    def print_usage():
        print("USAGE: ")
        print("python dindex_builder.py [--load | --build] --profile_data_path <path>")
        exit()

    parser = argparse.ArgumentParser()
    parser.add_argument('--profile_data_path', default=None, help='Path to profile data')
    parser.add_argument('--build', action='store_true', help='build discovery index from profile_data_path')
    parser.add_argument('--force', action='store_true', help='build discovery index by removing previous one if necessary')
    parser.add_argument('--load', action='store_true', help='load existing discovery index')

    args = parser.parse_args()

    if args.build:
        if not args.profile_data_path:
            print_usage()
        dindex = build_dindex(args.profile_data_path, cnf, force=args.force)
    elif args.load:
        dindex = load_dindex(cnf)
    else:
        print_usage()

    # TODO: notification
