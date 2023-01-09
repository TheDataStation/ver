from typing import Dict

import duckdb

from dindex_store.common import DiscoveryIndex, EdgeType


class DiscoveryIndexDuckDB(DiscoveryIndex):

    def __init__(self, directory: str = '', testing: bool = False):
        # connect to DuckDB
        # TODO: pass connection parameters as a config object
        self.conn = duckdb.connect()

        # Create schema
        # FIXME: such schema would be also read from some external config file, although we dont need to do that
        # FIXME: until we settle on what the schema must look like
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
            from_node DECIMAL(18, 0),
            to_node   DECIMAL(18, 0),
            weight    DECIMAL(10, 4)
        ) ''')

    def add_node(self, node: Dict) -> bool:
        pass

    def add_edge(self, source: int, target: int, type: EdgeType, properties: Dict) -> bool:
        pass

    def add_undirected_edge(self, source: int, target: int, type: EdgeType, weigth: float) -> bool:
        pass


if __name__ == "__main__":
    print("Discovery Index using a DuckDB backend")
