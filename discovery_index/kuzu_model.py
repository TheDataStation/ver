import kuzu
import argparse

class DiscoveryGraph:
    def __init__(self, directory: str='', testing: bool=False):
        self.db = kuzu.database('./test')
        self.conn = kuzu.connection(self.db)
        self.conn.execute("CREATE NODE TABLE Column(id INT64, attr INT64, PRIMARY KEY (id))")
        self.conn.execute("CREATE REL TABLE Edge(FROM Column TO Column, similar INT64)")

        if testing:
            return
    
    def add_node(self, id):
        self.conn.execute(f'CREATE (n:Column {{id: {id}, attr: 0}})')
    
    def add_undirected_edge(self, node_1: float, node_2: float):
        self.conn.execute(
            f'''MATCH
            (a:Column),
            (b:Column)
            WHERE a.id = {node_1} AND b.id = {node_2}
            CREATE (a)-[r:Edge {{similar: 1}}]->(b)
            '''
        )
        self.conn.execute(
            f'''MATCH
            (a:Column),
            (b:Column)
            WHERE a.id = {node_1} AND b.id = {node_2}
            CREATE (b)-[r:Edge {{similar: 1}}]->(a)
            '''
        )

    def find_neighborhood(self, node, hops=2):
        results = self.conn.execute(
            f'''MATCH
            (startNode:Column {{id : {node}}})-[:Edge*1..{hops}]->(endNode:Column)
            RETURN endNode''').getAsDF()
        print(results)
        return results

    def find_path(self, start, end):
        results = self.conn.execute(
            f'''MATCH
            (startNode:Column {{id : {start}}})-[:Edge*1..5]->
            (endNode:Column {{id : {end}}})
            RETURN COUNT(*)''').getAsDF()
        print(results)
        return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog = 'Network Builder',
                    description = 'Builds the Entreprise Knowledge Graph')
    parser.add_argument('-p', '--path',
                        help='the directory that stores column profiles in JSON'
                             'format')
    
    args = parser.parse_args()
    g = DiscoveryGraph(testing=True)
    for i in range(5):
        g.add_node(i)
    
    g.add_undirected_edge(0, 1)
    g.add_undirected_edge(1, 2)
    g.add_undirected_edge(2, 3)
    g.add_undirected_edge(3, 4)
    
    # g.find_neighborhood(1)
    # g.find_path(0, 3)