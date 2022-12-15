from abc import ABC
from enum import Enum


class EdgeType(Enum):
    ATTRIBUTE_SYNTACTIC_SIMILARITY = 0
    COLUMN_CONTENT_JACCARD_SIMILARITY = 1
    SYNTACTIC_INCLUSION_DEPENDENCY = 2


class DiscoveryIndex(ABC):

    def __init__(self):
        return

    def add_node(self, node: dict) -> bool:
        return

    def add_edge(self, source: int, target: int, type: EdgeType, weigth: float) -> bool:
        return

    def add_undirected_edge(self, source: int, target: int, type: EdgeType, weigth: float) -> bool:
        return


class FullTextSearchIndex(ABC):

    def init(self):
        return


class MinHashIndex(ABC):

    def init(self):
        return


if __name__ == "__main__":
    print("dindex-store : common")
