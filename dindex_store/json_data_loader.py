import os
import json

from dindex_store.discovery_index import DiscoveryIndex


def load_data(path, discovery_graph: DiscoveryIndex):
    # Populate the nodes table with JSON files under the specified path
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            with open(filepath) as f:
                profile = json.load(f)
                discovery_graph.add_profile(profile)


if __name__ == "__main__":
    print("Loads JSON data into discovery index")

    # TODO
    # pick path and pick graph for the function above to work
