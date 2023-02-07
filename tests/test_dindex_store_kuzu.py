import sys
import pytest
import subprocess

sys.path.append('../')

from dindex_store.common import EdgeType
from dindex_store.dindex_store_kuzu import DiscoveryIndexKuzu
from dindex_store.json_data_loader import load_data

@pytest.fixture
def clean_up():
    yield
    subprocess.run(["rm", "-rf", "./temp"])

def test_load_data(clean_up):
    discovery_graph = DiscoveryIndexKuzu(schema_path="test_files/kuzu_schema.txt")
    load_data("test_files/data/", discovery_graph)

def test_chain_graph(clean_up):
    discovery_graph = DiscoveryIndexKuzu(schema_path="test_files/kuzu_dummy_schema.txt")
    for i in range(5):
        discovery_graph.add_node({"id": i, "attr": 1})
    for i in range(4):
        discovery_graph.add_undirected_edge(i, i + 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"similar": 1})
    
    result = discovery_graph.find_neighborhood(1)
    assert result.shape[0] == 2

    
    
    