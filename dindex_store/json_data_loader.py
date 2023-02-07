import os
import json

from dindex_store.discovery_index import DiscoveryIndex


def load_data(path, graph: DiscoveryIndex):
    # Populate the nodes table with JSON files under the specified path
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            with open(filepath) as f:
                column = json.load(f)
                graph.add_node(column)


if __name__ == "__main__":
    print("Loads JSON data into discovery index")

    # TODO
    # pick path and pick graph for the function above to work
