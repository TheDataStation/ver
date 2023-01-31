from typing import Dict

import os
import json
import kuzu
import pytest

from dindex_store.common import DiscoveryIndex, EdgeType


class DiscoveryIndexKuzu(DiscoveryIndex):

    def __init__(self, directory: str = '', testing: bool = False):
        # connect to Kuzu
        # TODO: pass connection parameters as a config object
        self.conn = kuzu.database('./test')
        self.conn = kuzu.connection(self.db)

        # Create schema
        # FIXME: such schema would be also read from some external config file, although we dont need to do that
        # FIXME: until we settle on what the schema must look like
        self.conn.execute("""
        CREATE NODE TABLE Column(id INT64, attr INT64, PRIMARY KEY (id))
        """)
        self.conn.execute("""
        CREATE REL TABLE Edge(FROM Column TO Column, similar INT64)
        """)

        if testing:
            return
        
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                with open(filepath) as f:
                    column = json.load(f)
                    self.add_node(column)
        
    def add_node(self, node: Dict) -> bool:
        id = node['id']
        self.conn.execute(f'CREATE (n:Column {{id: {id}, attr: 0}})')

    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        self.conn.execute(
            f'''MATCH
            (a:Column),
            (b:Column)
            WHERE a.id = {source} AND b.id = {target}
            CREATE (a)-[r:Edge {{similar: 1}}]->(b)
            ''')

    def add_undirected_edge(self, source: int, target: int, type: EdgeType, weight: float) -> bool:
        self.add_edge(source, target, type)
        self.add_edge(target, source, type)

    def find_neighborhood(self, node, hops=2):
        results = self.conn.execute(
            f'''MATCH
            (startNode:Column {{id : {node}}})-[:Edge*1..{hops}]->(endNode:Column)
            RETURN endNode''').getAsDF()
        return results

    def find_path(self, source: int, target: int):
        results = self.conn.execute(
            f'''MATCH
            (startNode:Column {{id : {source}}})-[:Edge*1..5]->
            (endNode:Column {{id : {target}}})
            RETURN COUNT(*)''').getAsDF()
        return results

# FIXME: Need more tests
def test_find_neighborhood(a, x, expected):
    """
    Do a single test for find_neighborhood
    """
    g = DiscoveryIndexKuzu(testing=True)
    for i in range(5):
        g.add_node(i)
    for i in range(4):
        g.add_undirected_edge(i, i + 1)

    assert g.find_neighborhood(1)[count] == 3

if __name__ == "__main__":
    print("Discovery Index using a Kuzu backend")

