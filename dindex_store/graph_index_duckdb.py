from typing import Dict, List

import duckdb
import pandas as pd

from dindex_store.common import GraphIndex, EdgeType


class GraphIndexDuckDB(GraphIndex):

    def __init__(self, config: Dict) -> None:
        GraphIndexDuckDB._validate_config(config)
        self.config = config
        self.conn = duckdb.connect(database=config["graph_duckdb_database_name"])
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
        pass

    def add_edge(
            self,
            source: int,
            target: int,
            type: EdgeType,
            properties: Dict) -> bool:
        edges_table = self.conn.table('edges')
        edges_table.insert([source, target] + list(properties.values()))

    def add_undirected_edge(
            self,
            source: int,
            target: int,
            type: EdgeType,
            properties: Dict) -> bool:
        edges_table = self.conn.table('edges')
        edges_table.insert([source, target] + list(properties.values()))
        edges_table.insert([target, source] + list(properties.values()))

    # ----------------------------------------------------------------------
    # Query Methods

    def _get_paths(self, node_id: int, len: int) -> pd.DataFrame:
        return self.conn.execute(
            f'''
        WITH RECURSIVE paths(startNode, endNode, path) AS (
            SELECT
                    from_node AS startNode,
                    to_node AS endNode,
                    [from_node, to_node] AS path
                FROM edges
                WHERE startNode = {node_id}
            UNION ALL
            SELECT
                    paths.startNode AS startNode,
                    to_node AS endNode,
                    array_append(path, to_node) AS path
                FROM paths
                JOIN edges ON paths.endNode = from_node
                WHERE NOT EXISTS (SELECT 1
                                FROM paths previous_paths
                                WHERE list_contains(previous_paths.path, to_node))
                AND length(paths.path) <= {len}
        )
        SELECT startNode, endNode, path
        FROM paths
        ORDER BY length(path), path;
        '''
        ).fetch_df()

    def find_neighborhood(self, node_id: int, hops=1) -> List:
        try:
            df = self._get_paths(node_id, hops)
            return df["endNode"].tolist()
        except:
            print(f"Error when trying to find a {hops}-hop neighborhood")
            return []

    def find_path(
            self,
            source_node_id: int,
            target_node_id: int,
            max_len: int) -> List:
        try:
            df = self._get_paths(source_node_id, max_len)
            return df[df["endNode"] == target_node_id]["path"].tolist()
        except:
            print(f"Error when trying to find paths between "
                  "{source_node_id} to {target_node_id}")
            return []

    @classmethod
    def _validate_config(cls, config):
        pass