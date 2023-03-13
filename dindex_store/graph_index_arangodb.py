import numpy

from typing import Dict, List
from arango import ArangoClient

from dindex_store.common import GraphIndex, EdgeType

class GraphIndexArangoDB(GraphIndex):
    def __init__(self):
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

    # ----------------------------------------------------------------------
    # Modify Methods

    def add_node(self, node: Dict) -> bool:
        try:
            if 'id' in node:
                node['_key'] = str(node['id'])

            self.nodes.insert(node)
            return True
        except:
            print("Error when creating the node")
            return False

    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        try:
            properties.update({'_from': f'nodes/{source}', '_to': f'nodes/{target}'})
            self.edges.insert(properties)
            return True
        except:
            print("Error when creating the edge")
            return False
    
    def add_undirected_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        # Undirected edges will be stored as directed edges and the query will use undirected mode
        return self.add_edge(source, target, type, properties)

    def delete_graph(self):
        self.sys_db.delete_database('graph')

    # ----------------------------------------------------------------------
    # Query Methods

    def find_neighborhood(self, node_id: int, hops=1) -> List:
        try:
            query = f'''FOR v, e, p IN 1..{hops} ANY \'nodes/{node_id}\' GRAPH \'graph\'
                            OPTIONS {{ order: "bfs", uniqueVertices: "path" }}
                            RETURN p.vertices[*]._key'''
            cursor = self.db.aql.execute(query)
            return numpy.array([doc[1] for doc in cursor])
        except:
            print(f"Error when trying to find a {hops}-hop neighborhood")

    def find_path(
            self,
            source_id: int,
            target_id: int,
            max_len: int) -> List:
        try:
            query = f'FOR p IN ANY ALL_SHORTEST_PATHS \'nodes/{source_id}\' TO \'nodes/{target_id}\' GRAPH \'graph\' RETURN p.vertices[*]._key'
            cursor = self.db.aql.execute(query)
            return numpy.array([doc for doc in cursor if len(doc) <= max_len])
        except:
            print(f"Error when trying to find paths between {source_id} to {target_id}")

if __name__ == "__main__":
    print("Discovery Index using a ArangoDB backend")
