from typing import Dict

import duckdb

from dindex_store.common import DiscoveryIndex, EdgeType

class DiscoveryIndexDuckDB(DiscoveryIndex):

    def __init__(self, schema_path: str):
        # connect to DuckDB
        # TODO: pass connection parameters as a config object
        self.conn = duckdb.connect()

        # Create schema
        # FIXME: such schema would be also read from some external config file, although we dont need to do that
        # FIXME: until we settle on what the schema must look like
        assert schema_path, "Error: schema_path is missing"
        with open(schema_path) as f:
            schema = f.read()
        try:
            for statement in schema.split(";"):
                self.conn.execute(statement)
        except:
            print("An error has occurred when reading the schema")

    def add_node(self, node: Dict) -> bool:
        if node['minhash']:
            if not self.minhash_perm:
                self.minhash_perm = len(node['minhash'])
            node['minhash'] = ','.join(map(str, node['minhash']))

        nodes_table = self.conn.table('nodes')
        nodes_table.insert(node.values())

    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        edges_table = self.conn.table('edges')
        edges_table.insert([source, target] + properties.values())

    def add_undirected_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        edges_table = self.conn.table('edges')
        edges_table.insert([source, target] + properties.values())
        edges_table.insert([target, source] + properties.values())

    """
    Read functions
    """

    def find_neighborhood(self, node_id: int, hops=1):
        try:
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
                    AND length(paths.path) <= {hops}
            )
            SELECT startNode, endNode, path
            FROM paths
            ORDER BY length(path), path;
            '''
            ).fetch_df()
        except:
            print(f"Error when trying to find a {hops}-hop neighborhood")

    def find_path(self, source_id: int, target_id: int):
        try:
            return self.conn.execute(
            f'''
            WITH RECURSIVE paths(startNode, endNode, path, endReached) AS (
                SELECT
                        from_node AS startNode,
                        to_node AS endNode,
                        [from_node, to_node] AS path,
                        (to_node = {target_id}) AS endReached
                    FROM edges
                    WHERE startNode = {source_id}
                UNION ALL
                SELECT
                        paths.startNode AS startNode,
                        to_node AS endNode,
                        array_append(path, to_node) AS path,
                        max(CASE WHEN to_node = {target_id} THEN 1 ELSE 0 END)
                            OVER (ROWS BETWEEN UNBOUNDED PRECEDING
                                        AND UNBOUNDED FOLLOWING) AS endReached
                    FROM paths
                    JOIN edges ON paths.endNode = from_node
                    WHERE NOT EXISTS (SELECT 1
                                    FROM paths previous_paths
                                    WHERE list_contains(previous_paths.path, to_node))
                    AND paths.endReached = 0
            )
            SELECT startNode, endNode, path
            FROM paths
            WHERE endNode = {target_id}
            '''
            ).fetch_df()
        except:
            print(f"Error when trying to find paths between {source_id} to {target_id}")

if __name__ == "__main__":
    print("Discovery Index using a DuckDB backend")
