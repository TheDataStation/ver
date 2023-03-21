from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List


class EdgeType(Enum):
    ATTRIBUTE_SYNTACTIC_SIMILARITY = 0
    COLUMN_CONTENT_JACCARD_SIMILARITY = 1
    SYNTACTIC_INCLUSION_DEPENDENCY = 2


class ProfileIndex(ABC):
    """
    Base class for profile indexes that stores statitics about a column.
    """

    @abstractmethod
    def add_profile(self, node: Dict) -> bool:
        """
        Adds profile, i.e. a column in the input data, to the index.

        :param node: Dictionary describing the profile
        :return: True if the operation succeeds
        """
        pass

    @abstractmethod
    def get_profile(self, node_id: int) -> Dict:
        """
        Get node dictionary given the node id.
        """
        pass

    @abstractmethod
    def get_minhashes(self) -> Dict:
        """
        Get all profiles with minhash for indexing
        """
        pass


class GraphIndex(ABC):
    """
    Base class for representing the graph structure between profiles.
    """

    @abstractmethod
    def add_node(self, node_id: int) -> bool:
        """
        Adds node with node_id to the graph.
        """
        pass

    @abstractmethod
    def add_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        """
        Add edge between two nodes in the graph. The edge is of type EdgeType.

        :param source_node_id: source node
        :param target_node_id: target node
        :param type: EdgeType
        :param properties: the properties of the edge as a dictionary
        :return: True if the operation succeeds
        """
        pass

    @abstractmethod
    def add_undirected_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        """
        Syntactic sugar over add_edge. Adding an undirected edge between A
        and B amounts to adding an edge from A to B and another edge
        from B to A.
        """
        pass

    @abstractmethod
    def find_neighborhood(self, node_id, hops) -> List:
        """
        Find a n-hop neighborhood around the node in the graph.

        :param node_id: node_id
        :param hops: maximum num of hops of the neighbohood
        :return: a list of neighbors
        """
        pass

    @abstractmethod
    def find_path(
            self,
            source_node_id: int,
            target_node_id: int,
            max_len: int) -> List:
        """
        Find paths that are no longer than max_len hops between source node and
        target node.

        :param source_node_id: source node id / start
        :param target_node_id: target node id / end
        :return: a list where each element is a path from source to target
        """
        pass


class FullTextSearchIndex(ABC):
    """
    Base class for full text search index.
    """

    @abstractmethod
    def insert(self, profile_id, dbName, path, sourceName, columnName, data) -> bool:
        pass

    @abstractmethod
    def fts_query(self, keyword, search_domain, max_results, exact_search) -> List:
        pass


class ContentSimilarityIndex(ABC):
    def init(self):
        return


if __name__ == "__main__":
    print("dindex-store : common")
