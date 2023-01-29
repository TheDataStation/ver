import os
import time
import random
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from arango import ArangoClient
from datasketch import MinHash, MinHashLSH

class ArangoDiscoveryGraph:
    def __init__(self, directory: str='', testing: bool=False):
        client = ArangoClient()
        self.sys_db = client.db('_system', username='root', password='password')

        if self.sys_db.has_database('graph'):
            self.sys_db.delete_database('graph')

        if not self.sys_db.has_database('graph'):
            self.sys_db.create_database(
                name='graph',
                users=[
                    {'username': 'user', 'password': 'password', 'active': True},
                ],
            )

        self.db = client.db('graph', username='user', password='password')
        self.sys_db.update_permission(username='user', permission='rw', database='graph')

        # Create graph
        if self.db.has_graph('graph'):
            self.graph = self.db.graph('graph')
        else:
            self.graph = self.db.create_graph('graph')

        # Create vertex collection
        if self.graph.has_vertex_collection('nodes'):
            self.nodes = self.graph.vertex_collection('nodes')
        else:
            self.nodes = self.graph.create_vertex_collection('nodes')

        # Create edge definition
        if not self.graph.has_edge_definition('edges'):
            teach = self.graph.create_edge_definition(
                edge_collection='edges',
                from_vertex_collections=['nodes'],
                to_vertex_collections=['nodes']
            )

        self.edges = self.graph.edge_collection('edges')

        if testing:
            return

        self.minhash_perm = None
        # Populate the nodes table with JSON files under the specified path
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                with open(filepath) as f:
                    column = json.load(f)
                    self.add_node(column)
        
        self.make_similarity_edges()

    # TODO: Implement
    def make_similarity_edges(self, threshold: int=0.5):
        ''''
        Construct the graph (edges) based on minHash signatures of the nodes
        '''
        pass

    def make_pkfk_edges(self):
        pass

    def make_neighbor_edges(self):
        pass

    def add_node(self, column: dict):
        '''
        Adds a single node, representing a column, into the nodes graph
        '''
        if 'id' in column:
            column['_key'] = str(column['id'])
        
        self.nodes.insert(column)
    
    # FIXME: Range of node id might not fit in a float
    def add_undirected_edge(self, node_1: float, node_2: float, weight: int=1):
        '''
        Add a single undirected edge into the edges table
        '''
        self.edges.insert({'_from': f'nodes/{node_1}', '_to': f'nodes/{node_2}', 'weight': weight})

    # TODO: Test implementation
    def find_neighborhood(self, node, hops):
        '''
        Find the n-hop neighborhood of a node
        '''
        # BFS on average is faster than DFS
        # uniqueVertices "global" is faster than "path"
        query = f'''FOR v, e, p IN 1..{hops} ANY \'nodes/{node}\' GRAPH \'graph\'
                        OPTIONS {{ order: "bfs", uniqueVertices: "global" }}
                        RETURN p.vertices[*]._key'''
        cursor = self.db.aql.execute(query)
        return [doc for doc in cursor]

    def find_path(self, start, end):
        '''
        Find a path between the start to the end
        '''
        query = f'FOR p IN ANY ALL_SHORTEST_PATHS \'nodes/{start}\' TO \'nodes/{end}\' GRAPH \'graph\' RETURN p.vertices[*]._key'
        cursor = self.db.aql.execute(query)
        return [doc for doc in cursor]

    def print_nodes(self):
        # FIXME: this function exists only for debugging purposes
        pass

    def delete_database(self):
        self.sys_db.delete_database('graph')

    def populate_scalability_test(self, num_nodes, sparsity):
        '''
        Populate the nodes collection with random nodes
        '''
        nodes_query = f'FOR i IN 0..{num_nodes - 1} INSERT {{\'_key\': TO_STRING(i)}} INTO nodes'
        cursor = self.db.aql.execute(nodes_query)

        edges_query = f'''FOR i IN 0..{num_nodes - 1}
                            FOR j IN i+1..{num_nodes - 1}
                              FILTER RAND() < {sparsity}
                                INSERT {{\'_from\': CONCAT(\'nodes/\', TO_STRING(i)), \'_to\': CONCAT(\'nodes/\', TO_STRING(j))}} INTO edges'''
        cursor = self.db.aql.execute(edges_query)

def test_graph(num_nodes, sparsity):
    '''
    Generate a random graph with given number of nodes and sparsity
    '''
    graph = ArangoDiscoveryGraph(testing=True)

    print(f"Populating a {num_nodes} node, {sparsity} sparsity graph")
    pop_start = time.time()
    graph.populate_scalability_test(num_nodes, sparsity)
    pop_end = time.time()

    print(f"The 2-hop neighborhood of 0 in a {num_nodes} node, {sparsity} "
           "sparsity graph is")
    nbhd_start = time.time()
    print(graph.find_neighborhood(0, 2))
    nbhd_end = time.time()

    print(f"All paths between node 0 and node 2 in a {num_nodes} node, "
          f"{sparsity} sparsity graph are")
    path_start = time.time()
    print(graph.find_path(0, 2))
    path_end = time.time()

    graph.delete_database()
    return pop_end - pop_start, nbhd_end - nbhd_start, path_end - path_start

def test_scalability():
    '''
    Testing the scalability of finding the 2-hop neighborhood of a node as well 
    as finding paths between 2-nodes
    '''
    nodes = [100, 200, 400, 800, 1600]
    sparsity = [0.1, 0.2]
    pop_result = []
    nbhd_result = []
    path_result = []
    titles = ['Populate Graph', '2-hop Neighborhood Search', 'Path Finding']

    for n in nodes:
        for s in sparsity:
            # nbhd_time, path_time = np.mean([test_graph(n, s) for i in range(3)], axis=0)
            pop_time, nbhd_time, path_time = test_graph(n, s)
            pop_result.append([n, s, pop_time])
            nbhd_result.append([n, s, nbhd_time])
            path_result.append([n, s, path_time])

    for result, title in zip([pop_result, nbhd_result, path_result], titles):
        df = pd.DataFrame(result, columns = ['No. of Nodes', 'Sparsity', 'Time'])
        df = df.pivot(index='No. of Nodes', columns='Sparsity', values='Time')
        plt.figure()
        df.plot(title=f'{title} Scalability')
        plt.xticks(nodes)
        plt.xlabel('No. of Nodes')
        plt.ylabel('Time')
        plt.savefig(f'{title}.png')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog = 'Network Builder',
                    description = 'Builds the Entreprise Knowledge Graph')
    parser.add_argument('-p', '--path',
                        help='the directory that stores column profiles in JSON'
                             'format')
    parser.add_argument('-t', '--test', action='store_true')

    args = parser.parse_args()
    test_scalability()
