from typing import Dict, List

from dindex_store.common import EdgeType
from dindex_store.profile_index_duckdb import ProfileIndexDuckDB
from dindex_store.graph_index_duckdb import GraphIndexDuckDB
from dindex_store.graph_index_kuzu import GraphIndexKuzu
from dindex_store.fulltext_index_duckdb import FTSIndexDuckDB
from dindex_store.content_similarity_index_minhash import SimpleMHIndex
from dindex_store.content_similarity_index_minhash import MHIndex


class DiscoveryIndex:
    """DiscoveryIndex stores profiles generated by the profiler, a graph that
    describes relationships between profiles, and a full text search index on
    the original input data.

    :param profile_index: ProfileIndex
    :param graph_idnex: GraphIndex
    :param fts_index: FullTextSearchIndex
    """

    """
    Mapping of index type and implementations
    """
    profile_index_mapping = {
        "duckdb": ProfileIndexDuckDB,
    }

    fts_index_mapping = {
        "duckdb": FTSIndexDuckDB,
    }

    content_similarity_index_mapping = {
        "simpleminhash": SimpleMHIndex,
        "minhash": MHIndex
    }

    graph_index_mapping = {
        "duckdb": GraphIndexDuckDB,
        "kuzu": GraphIndexKuzu,
    }

    def __init__(self, config: Dict, load=False) -> None:
        # TODO: Validate config in a consistent way
        self.__profile_index = DiscoveryIndex.profile_index_mapping[config["profile_index"]](config, load=load)
        self.__content_similarity_index = DiscoveryIndex.content_similarity_index_mapping[config["content_index"]](config, load=load)
        self.__fts_index = DiscoveryIndex.fts_index_mapping[config["fts_index"]](config, load=load)
        self.__graph_index = DiscoveryIndex.graph_index_mapping[config["graph_index"]](config, load=load)

    def get_content_similarity_index(self):
        return self.__content_similarity_index

    # ----------------------------------------------------------------------
    # Modify Methods

    def add_profile(self, profile: Dict) -> bool:
        profile_id = profile["id"]

        success_profile = self.__profile_index.add_profile(profile)
        if not success_profile:
            return False
        success_graph = self.__graph_index.add_node(profile_id)
        if not success_graph:
            return False
        success_content_similarity = self.__content_similarity_index.add_profile(profile_id, profile['minhash'])
        if not success_content_similarity:
            return False
        return True

    def add_text_content(self, row: Dict) -> bool:
        return self.__fts_index.insert(row)

    def add_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        return self.__graph_index.add_edge(
            source_node_id, target_node_id, type, properties)

    def add_undirected_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        return self.__graph_index.add_undirected_edge(
            source_node_id, target_node_id, type, properties)

    # ----------------------------------------------------------------------
    # Query Methods

    def get_profile(self, node_id: int) -> Dict:
        return self.__profile_index.get_profile(node_id)

    def get_filtered_profiles_from_table(self, table_name, desired_attributes):
        results = self.__profile_index.get_filtered_profiles_from_table(table_name, desired_attributes)
        return results

    def get_filtered_profiles_from_nids(self, nids, desired_attributes):
        results = self.__profile_index.get_filtered_profiles_from_nids(nids, desired_attributes)
        return results

    def get_minhashes(self) -> Dict:
        return self.__profile_index.get_minhashes()

    def find_neighborhood(self, node_id: int, relation_type, hops: int = 1, desired_attributes=None):
        neighbors_ids = self.__graph_index.find_neighborhood(node_id, relation_type, hops)
        # now, for each neighbor_id, retrieve the desired attributes
        if desired_attributes is not None:
            return self.__profile_index.get_filtered_profiles_from_nids(neighbors_ids, desired_attributes)
        else:
            return self.__profile_index.get_profile(neighbors_ids)

    def find_path(
            self,
            source_node_id: int,
            target_node_id: int,
            max_len: int = 3):
        return self.__graph_index.find_path(
            source_node_id, target_node_id, max_len)

    # TODO: search over "search_domain", return top-"max_results",
    # and switch between exact/approx search ("exact_search")
    def fts_query(self, keyword) -> List:
        return self.__fts_index.query(keyword)
