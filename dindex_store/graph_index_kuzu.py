from typing import Dict, List

import kuzu

from dindex_store.discovery_index import GraphIndex, EdgeType


class GraphIndexKuzu(GraphIndex):

    def __init__(self, config: Dict):
        GraphIndexKuzu._validate_config(config)
        self.config = config
        self.db = kuzu.database(config["kuzu_folder"])
        self.conn = kuzu.connection(self.db)
        self.schema = ""

        with open(config["graph_schema_path"]) as f:
            self.schema = f.read()
        try:
            for statement in self.schema.split(";"):
                self.conn.execute(statement)
        except:
            print("An error has occurred when reading the schema")
            raise

    # ----------------------------------------------------------------------
    # Modify Methods

    def add_node(self, node_id: int) -> bool:
        try:
            self.conn.execute(f'CREATE (n:Column {{ id: {node_id} }})')
            return True
        except:
            print("Error when creating the node")
            return False

    def add_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        try:
            attr = []
            for key, value in properties.items():
                if isinstance(value, str):
                    attr.append(key + ': ' + '"' + value + '"')
                else:
                    attr.append(key + ': ' + str(value))
            self.conn.execute(
                f'''MATCH (source:Column), (target:Column)
                WHERE source.id = {source_node_id} AND target.id = {target_node_id}
                CREATE (source)-[r:Edge {{ {", ".join(attr)} }}]->(target)
                ''')
            return True
        except:
            print("Error when creating the edge")
            return False

    def add_undirected_edge(
            self,
            source_node_id: int,
            target_node_id: int,
            type: EdgeType,
            properties: Dict) -> bool:
        self.add_edge(source_node_id, target_node_id, type, properties)
        self.add_edge(target_node_id, source_node_id, type, properties)

    # ----------------------------------------------------------------------
    # Query Methods

    def find_neighborhood(self, node_id, hops) -> List:
        try:
            results = self.conn.execute(
                f'''MATCH
                (startNode:Column {{id : {node_id}}})-[:Edge*1..{hops}]->(endNode:Column)
                RETURN endNode''').getAsDF()
            return results["endNode.id"].tolist()
        except:
            print(f"Error when trying to find a {hops}-hop neighborhood")
            return []

    def find_path(
            self,
            source_node_id: int,
            target_node_id: int,
            max_len: int) -> List:
        try:
            results = self.conn.execute(
                f'''MATCH
                (startNode:Column {{id : {source_node_id}}})-[:Edge*1..5]->
                (endNode:Column {{id : {target_node_id}}})
                RETURN COUNT(*)''').getAsDF()
            print(results)
            return []
        except:
            print(
                f"Error when trying to find paths between {source_node_id} to {target_node_id}")
            return []

    @classmethod
    def _validate_config(cls, config):
        pass