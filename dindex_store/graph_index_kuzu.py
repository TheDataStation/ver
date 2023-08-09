from typing import Dict, List
from pathlib import Path
import kuzu
# from kuzu import BinderException

from dindex_store.common import GraphIndex, EdgeType


class GraphIndexKuzu(GraphIndex):

    def __init__(self, config: Dict, load=False, force=False):
        GraphIndexKuzu._validate_config(config)
        self.config = config
        db_path = Path(config['ver_base_path'] / config['graph_kuzu_database_name']).absolute()
        self.db = kuzu.Database(str(db_path))
        self.conn = kuzu.Connection(self.db)
        self.schema = ""

        if not load:
            # graph_schema_path = Path(os.getcwd() + "/" + config["graph_schema_name"]).absolute()
            graph_schema_path = Path(config['ver_base_path'] / config['graph_schema_name']).absolute()
            if not graph_schema_path.is_file():
                raise ValueError("The path to graph_schema does not exist, or is not a file")
            with open(graph_schema_path) as f:
                self.schema = f.read()

            if force:
                try:
                    # Note Edge and Column are hardcoded in the schema
                    q = f"DROP TABLE Edge;"
                    self.conn.execute(q)
                    q = f"DROP TABLE ColumnNode;"
                    self.conn.execute(q)
                except RuntimeError as re:
                    if re == "Binder exception: Node/Rel Edge does not exist.":
                        print("An error has occurred when reading the schema, probably using "
                              "--force with the first run of --build")
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
            self.conn.execute(f'CREATE (n:ColumnNode {{ id: {node_id} }})')
            return True
        except Exception as e:
            print(f"Error when creating the node: {e}")
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
                f'''MATCH (source:ColumnNode), (target:ColumnNode)
                WHERE source.id = {source_node_id} AND target.id = {target_node_id}
                CREATE (source)-[r:Edge {{ {", ".join(attr)} }}]->(target)
                ''')
            return True
        except Exception as e:
            print(f"Error when creating the edge: {e}")
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
                (startNode:ColumnNode {{id : {node_id}}})-[:Edge*1..{hops}]->(endNode:ColumnNode)
                RETURN endNode''')
            neighbors = []
            while results.has_next():
                neighbors.append(results.get_next()[0]['id'])
            return neighbors
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