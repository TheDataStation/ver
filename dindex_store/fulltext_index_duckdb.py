from typing import List

import duckdb

from dindex_store.common import FullTextSearchIndex


class FTSIndexDuckDB(FullTextSearchIndex):

    def __init__(self, config, load=False):
        # FIXME: Validate Config Name
        self.config = config
        self.conn = duckdb.connect(database=config["fts_duckdb_database_name"])

        self.table_name = config["fts_data_table_name"]
        self.index_column = config["fts_index_column"]

        if not load:
            with open(config['fts_schema_path']) as f:
                self.schema = f.read()
            try:
                # if we are building the index then we have to create the schema and the index
                self.conn.execute(self.schema)
                # create fts index on index_column
                self.__create_fts_index(self.table_name, self.index_column)
            except:
                print("An error has occurred when reading the schema")
                raise

    # ----------------------------------------------------------------------
    # Modify Methods

    def __create_fts_index(self, table_name, index_column):
        # Create fts index over all, *, attributes
        query = f"PRAGMA create_fts_index('{table_name}', '{index_column}', '*', stopwords='english')"
        self.conn.execute(query)

        prepare_query = f"""
            PREPARE fts_query AS (
                WITH scored_docs AS (
                    SELECT *, fts_main_{table_name}.match_bm25(profile_id, ?) AS score FROM {table_name})
                SELECT profile_id, score
                FROM scored_docs
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT 100)
            """
        self.conn.execute(prepare_query)

    def insert(self, profile_id, dbName, path, sourceName, columnName, data) -> bool:
        try:
            fts_data_table = self.conn.table(self.table_name)
            fts_data_table.insert([profile_id, dbName, path, sourceName, columnName, data])
            return True
        except:
            print("An error has occured when trying to add text data")
            return False

    # ----------------------------------------------------------------------
    # Query Methods

    def fts_query(self, keyword, search_domain, max_results, exact_search) -> List:
        # TODO: search over "search_domain", return top-"max_results", and switch between exact/approx search ("exact_search")
        res = self.conn.execute("EXECUTE fts_query('" + keyword + "')")
        return res.fetchall()
