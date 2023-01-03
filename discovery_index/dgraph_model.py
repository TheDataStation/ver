import pydgraph
import argparse
import json
import random

# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')

# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def DiscoveryGraph():
    def __init__(self):
        self.client_stub = create_client_stub()
        self.client = create_client(self.client_stub)
        # Clear all data and start from a clean state
        self.client.alter(pydgraph.Operation(drop_all=True))

        # Set schema
        schema = '''
        type Column {
            label
            connected_to
            # minhash
        }

        # Define Directives and index

        label: string @index .
        connected_to: [uid] .
        # minhash: [int] .
        '''
        self.client.alter.alter(pydgraph.Operation(schema=schema,       run_in_background=True))
    
    def add_node(self, label):
        nquad = f'''
        _:node <label> {label}
        '''
        txn = self.client.txn()
        try:
            txn.mutate(set_nquads=nquad)
            txn.commit()
            print("Successful!")
        finally:
            txn.discard()

    def add_undirected_edge(self, node_1, node_2, weight=1):
        txn = self.client.txn()
        query = f'''{{
        node_1 as var(func: eq(label, {node_1}))
        node_2 as var(func: eq(label, {node_2}))
        }}'''
        nquad = f"""
        uid(node_1) <connected_to> uid(node_2) (weight={weight}) .
        uid(node_2) <connected_to> uid(node_1) (weight={weight}) .
        """
        mutation = txn.create_mutation(set_nquads=nquad)
        request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
        txn.do_request(request)
        
    def find_neighborhood(self, node, hops):
        query = """query neighborhood($node: string) {
                neighborhood(func: eq(label, $node)) {
                    label
                    neighbors: connected_to {
                        label
                    }
                }
            }"""
        variables = {'$node': 'Alice'}
        res = self.client.txn(read_only=True).query(query, variables=variables)

    def find_path(self, start, end):
        pass

def test_graph(num_nodes, sparsity):
    graph = DiscoveryGraph()

    for i in range(num_nodes):
        graph.add_node(str(i))

    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            if random.random() < sparsity:
                graph.add_undirected_edge(str(i), str(j))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog = 'Network Builder',
                    description = 'Builds the Entreprise Knowledge Graph')
    parser.add_argument('-p', '--path',
                        help='the directory that stores column profiles in JSON'
                             'format')
    
    test_graph(100, 0.1)