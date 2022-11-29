import os
import time
import json
import argparse
import duckdb
import numpy as np
from datasketch import MinHash, MinHashLSH

class DiscoveryGraph:
    def __init__(self, directory: str):
        self.conn = duckdb.connect()
        self.conn.execute(
        '''CREATE TABLE nodes (
            id             DECIMAL(18, 0) NOT NULL PRIMARY KEY,
            dbname         VARCHAR(255),
            path           VARCHAR(255),
            sourcename     VARCHAR(255),
            columnname     VARCHAR(255),
            datatype       VARCHAR(255),
            totalvalues    INT,
            uniquevalues   INT,
            nonemptyvalues INT,
            entities       VARCHAR(255),
            minhash        BLOB,
            minvalue       DECIMAL(18, 4),
            maxvalue       DECIMAL(18, 4),
            avgvalue       DECIMAL(18, 4),
            median         DECIMAL(18, 0),
            iqr            DECIMAL(18, 0)
        )''')
        self.conn.execute('''CREATE TABLE edges
        (
            from_node DECIMAL(18, 0) REFERENCES nodes(id),
            to_node   DECIMAL(18, 0) REFERENCES nodes(id),
            weight    DECIMAL(10, 4)
        ) ''')
        self.minhash_perm = None
        # Populate the nodes table with JSON files under the specified path
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                with open(filepath) as f:
                    column = json.load(f)
                    self.add_node(column)
        
        self.make_similarity_edges()

    def make_similarity_edges(self, threshold: int=0.5):
        ''''
        Construct the graph (edges) based on minHash signatures of the nodes
        '''
        content_index = MinHashLSH(threshold, num_perm=self.minhash_perm)
        relation = self.conn.table('nodes')
        
        # Get all nodes with minhash
        start_time = time.time()
        df = relation.filter('minhash IS NOT NULL') \
                     .project('id, decode(minhash)') \
                     .to_df() \
                     .rename(columns={'decode(minhash)' : 'minhash'})

        df['minhash'] = df['minhash']. \
            map(lambda x : MinHash(num_perm=self.minhash_perm,
                                   hashvalues=np.array(x.split(','))))
        df.apply(lambda row : content_index.insert(row['id'],
                                                   row['minhash']), axis=1)
        spent_time = time.time() - start_time
        print(f'Indexed all minHash signatures: Took {spent_time}')

        for _, row in df.iterrows():
            neighbors = content_index.query(row['minhash'])
            for neighbor in neighbors:
                # TODO: Need to check that they are not from the same source
                self.add_undirected_edge(row['id'], neighbor)

    def make_pkfk_edges(self):
        pass

    def make_neighbor_edges(self):
        pass

    def add_node(self, column: dict):
        '''
        Adds a single node, representing a column, into the nodes graph
        '''
        # TODO: Need to enforce that the order of the json fields match
        if column['minhash']:
            if not self.minhash_perm:
                self.minhash_perm = len(column['minhash'])
            column['minhash'] = ','.join(map(str, column['minhash']))

        nodes_table = self.conn.table('nodes')
        nodes_table.insert(column.values())
    
    # FIXME: Range of node id might not fit in a float
    def add_undirected_edge(self, node_1: float, node_2: float, weight: int=1):
        '''
        Add a single undirected edge into the edges table
        '''
        edges_table = self.conn.table('edges')
        edges_table.insert([node_1, node_2, weight])
        edges_table.insert([node_2, node_1, weight])

    # TODO: Test implementation
    def find_neighborhood(self, node, hops):
        '''
        Find the n-hop neighborhood of a node
        '''
        return self.conn.execute(
        f'''
        WITH RECURSIVE paths(startNode, endNode, path) AS (
            SELECT
                    from_node AS startNode,
                    to_node AS endNode,
                    [from_node, to_node] AS path
                FROM edges
                WHERE startNode = {node}
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

    def find_path(self, start, end):
        '''
        Find a path between the start to the end
        '''
        return self.conn.execute(
        f'''
        WITH RECURSIVE paths(startNode, endNode, path, endReached) AS (
            SELECT
                    from_node AS startNode,
                    to_node AS endNode,
                    [from_node, to_node] AS path,
                    (to_node = {end}) AS endReached
                FROM edges
                WHERE startNode = {start}
            UNION ALL
            SELECT
                    paths.startNode AS startNode,
                    to_node AS endNode,
                    array_append(path, to_node) AS path,
                    max(CASE WHEN to_node = {end} THEN 1 ELSE 0 END)
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
        WHERE endNode = {end}
        '''
        ).fetch_df()

    def print_nodes(self):
        # FIXME: this function exists only for debugging purposes
        print(self.conn.table('nodes').create_view('test_nodes_view'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog = 'Network Builder',
                    description = 'Builds the Entreprise Knowledge Graph')
    parser.add_argument('-p', '--path',
                        help='the directory that stores column profiles in JSON'
                             'format')
    
    args = parser.parse_args()
    discovery_graph = DiscoveryGraph(args.path)
