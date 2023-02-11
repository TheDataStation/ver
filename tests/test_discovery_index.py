import sys
import os
import pytest
import subprocess
import yaml

sys.path.append('../')

from dindex_builder.json_data_loader import load_data
from dindex_store.graph_index_duckdb import GraphIndexDuckDB
from dindex_store.graph_index_kuzu import GraphIndexKuzu
from dindex_store.profile_index_duckdb import ProfileIndexDuckDB
from dindex_store.discovery_index import DiscoveryIndex, EdgeType

kuzu_folder = os.getcwd() + "/temp"


def read_config(file_path):
    with open(file_path, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config


@pytest.fixture
def clean_up():
    yield
    subprocess.run(["rm", "-rf", kuzu_folder])


def test_load_data(clean_up):
    config = read_config("test_files/dindex_config.yml")
    config["profile_schema_path"] = "test_files/profile_index_schema_duckdb.txt"
    config["kuzu_folder"] = kuzu_folder
    discovery_graph = DiscoveryIndex(
        ProfileIndexDuckDB(config),
        GraphIndexKuzu(config))
    load_data("test_files/data/", discovery_graph)


def test_chain_graph_kuzu(clean_up):
    config = read_config("test_files/dindex_config.yml")
    config["kuzu_folder"] = kuzu_folder
    discovery_graph = DiscoveryIndex(
        ProfileIndexDuckDB(config),
        GraphIndexKuzu(config))

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})
    for i in range(4):
        discovery_graph.add_undirected_edge(
            i, i + 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"similar": 1})

    result = discovery_graph.find_neighborhood(1)
    assert len(result) == 2


def test_chain_graph_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    discovery_graph = DiscoveryIndex(
        ProfileIndexDuckDB(config),
        GraphIndexDuckDB(config))

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})
    for i in range(4):
        discovery_graph.add_undirected_edge(
            i, i + 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(1)
    assert len(result) == 2
