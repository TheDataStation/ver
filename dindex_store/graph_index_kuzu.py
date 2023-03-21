from typing import Dict, List
from pathlib import Path
import os

import kuzu

from dindex_store.common import GraphIndex, EdgeType


class GraphIndexKuzu(GraphIndex):

    def __init__(self, config: Dict, load=False, force=False):
        GraphIndexKuzu._validate_config(config)
        self.config = config
        self.db = kuzu.database(config["graph_kuzu_database_name"])
        self.conn = kuzu.connection(self.db)
        self.schema = ""

        if not load:
            graph_schema_path = Path(os.getcwd() + "/" + config["graph_schema_name"]).absolute()
            if not os.path.isfile(graph_schema_path):
                raise ValueError("The path to graph_schema does not exist, or is not a file")
            with open(graph_schema_path) as f:
                self.schema = f.read()
            try:
                if force:
                    # Note Edge and Column are hardcoded in the schema
                    q = f"DROP TABLE Edge;"
                    self.conn.execute(q)
                    q = f"DROP TABLE Column;"
                    self.conn.execute(q)
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

    def find_neighborhood(self, node_id, relation_type, hops) -> List:
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