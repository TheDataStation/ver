import sys
import os
import pytest
import subprocess
import yaml

# TODO: Remove once we have better folder organization
sys.path.append('../')

from dindex_builder.dindex_builder import load_json_data, dindex_builder
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

    discovery_graph = DiscoveryIndex(config)
    load_json_data("test_files/data/", discovery_graph)

def test_dindex_builder(clean_up):
    config = read_config("test_files/dindex_config.yml")
    config["kuzu_folder"] = kuzu_folder
    config["profile_schema_path"] = "test_files/profile_index_schema_duckdb.txt"
    discovery_graph = dindex_builder(config)

def test_chain_graph_kuzu(clean_up):
    config = read_config("test_files/dindex_config.yml")
    config["kuzu_folder"] = kuzu_folder
    discovery_graph = DiscoveryIndex(config)

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
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})
    for i in range(4):
        discovery_graph.add_undirected_edge(
            i, i + 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(1)
    assert len(result) == 2


def test_two_hop_single_path_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        0, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(0, 2)
    assert len(result) == 3


def test_two_hop_multiple_path_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        0, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        2, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(0, 2)
    assert len(result) == 4


def test_two_hop_shortest_path_only_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        0, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(0, 2)
    assert len(result) == 2


def test_correct_amount_of_hops_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(0, 1)
    assert len(result) == 1


def test_unconnected_neighbor_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        2, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_neighborhood(0, 2)
    assert len(result) == 1


def test_path_find_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_path(0, 2)
    assert len(result) == 1


def test_path_find_multiple_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        0, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        2, 3, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_path(0, 3)
    assert len(result) == 2


def test_path_find_shortest_only_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    discovery_graph.add_undirected_edge(
        0, 1, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        1, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})
    discovery_graph.add_undirected_edge(
        0, 2, EdgeType.ATTRIBUTE_SYNTACTIC_SIMILARITY, {"weight": 1})

    result = discovery_graph.find_path(0, 2)
    assert len(result) == 1


def test_path_find_unconnected_duckdb():
    config = read_config("test_files/dindex_config.yml")
    config["graph_schema_path"] = "test_files/graph_index_schema_duckdb.txt"
    config["graph_index"] = "duckdb"
    discovery_graph = DiscoveryIndex(config)

    for i in range(5):
        discovery_graph.add_profile({"id": i, "attr": 1})

    result = discovery_graph.find_path(0, 1)
    assert len(result) == 0
