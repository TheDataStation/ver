from typing import Dict

import os
import json
import kuzu

from dindex_store.common import DiscoveryIndex, EdgeType

class DiscoveryIndexKuzu(DiscoveryIndex):

    def __init__(self, schema_path: str):
        # connect to Kuzu
        # TODO: pass connection parameters as a config object
        self.db = kuzu.database('./temp')
        self.conn = kuzu.connection(self.db)

        # Create schema
        # FIXME: such schema would be also read from some external config file, although we dont need to do that
        # FIXME: until we settle on what the schema must look like
        assert schema_path, "Error: schema_path is missing"
        with open(schema_path) as f:
            schema = f.read()
        try:
            for statement in schema.split(";"):
                print(statement)
                self.conn.execute(statement)
        except:
            print("An error has occurred when reading the schema")
        
    def add_node(self, node: Dict) -> bool:
        try:
            attr = []
            for key, value in node.items():
                key = key.lower()
                if key == "minhash":
                    continue
                if isinstance(value, str):
                    attr.append(key +': ' + '"' + value + '"')
                elif isinstance(value, list):
                    attr.append(key +': ' + '"' + ", ".join(str(num) for num in value) + '"')
                else:
                    if key in ["minvalue", "maxvalue", "avgvalue", "median", "iqr"]:
                        value = float(value)
                    attr.append(key +': ' + str(value))
            self.conn.execute(f'CREATE (n:Column {{ {", ".join(attr)} }})')
            return True
        except:
            print("Error when creating the node")
            return False

    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        try:
            attr = []
            for key, value in properties.items():
                if isinstance(value, str):
                    attr.append(key +': ' + '"' + value + '"')
                else:
                    attr.append(key +': ' + str(value))
            self.conn.execute(
                f'''MATCH (source:Column), (target:Column)
                WHERE source.id = {source} AND target.id = {target}
                CREATE (source)-[r:Edge {{ {", ".join(attr)} }}]->(target)
                ''')
            return True
        except:
            print("Error when creating the edge")
            return False

    def add_undirected_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        self.add_edge(source, target, type, properties)
        self.add_edge(target, source, type, properties)

    def find_neighborhood(self, node_id: int, hops=1):
        try:
            results = self.conn.execute(
                f'''MATCH
                (startNode:Column {{id : {node_id}}})-[:Edge*1..{hops}]->(endNode:Column)
                RETURN endNode''').getAsDF()
            return results
        except:
            print(f"Error when trying to find a {hops}-hop neighborhood")

    def find_path(self, source_id: int, target_id: int):
        try:
            results = self.conn.execute(
                f'''MATCH
                (startNode:Column {{id : {source_id}}})-[:Edge*1..5]->
                (endNode:Column {{id : {target_id}}})
                RETURN COUNT(*)''').getAsDF()
            return results
        except:
            print(f"Error when trying to find paths between {source_id} to {target_id}")

if __name__ == "__main__":
    print("Discovery Index using a Kuzu backend")

