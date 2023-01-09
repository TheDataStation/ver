from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict


class EdgeType(Enum):
    ATTRIBUTE_SYNTACTIC_SIMILARITY = 0
    COLUMN_CONTENT_JACCARD_SIMILARITY = 1
    SYNTACTIC_INCLUSION_DEPENDENCY = 2


class DiscoveryIndex(ABC):
    """
    DiscoveryIndex is an abstract class designed to enforce consistency on its implementations. It abstract the
    discovery index management, that may be done via a library, database, or other. It exposes a series of methods
    common across implementations and that must be implemented by any class inheriting DiscoveryIndex.
    """

    @abstractmethod
    def add_node(self, node: Dict) -> bool:
        """
        Adds node to graph. Node is a python dictionary
        :param node: dictionary describing the node
        :return: true if the operation succeeds
        """
        pass

    @abstractmethod
    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        """
        Add edge between two nodes in the graph. The edge is of type EdgeType
        :param source: source node
        :param target: target node
        :param type: EdgeType
        :param properties: the properties of the edge as a dictionary, e.g., weight
        :return:
        """
        pass

    @abstractmethod
    def add_undirected_edge(self, source: int, target: int, type: EdgeType, weigth: float) -> bool:
        """
        Syntactic sugar over add_edge. Adding an undirected edge between A and B amounts to adding an edge
        between A and B and another edge between B and A.
        :param source:
        :param target:
        :param type:
        :param weigth:
        :return:
        """
        pass

    # TODO: extend with other functions that any DiscoveryIndex impl must implement, e.g., find-path, neighborhood, etc


class FullTextSearchIndex(ABC):

    def init(self):
        return


class MinHashIndex(ABC):

    def init(self):
        return


if __name__ == "__main__":
    print("dindex-store : common")
