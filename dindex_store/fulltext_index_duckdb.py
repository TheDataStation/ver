from typing import List

import duckdb

from dindex_store.common import FullTextSearchIndex


class FTSIndexDuckDB(FullTextSearchIndex):

    def __init__(self, config):
        # FIXME: Validate Config Name
        self.config = config
        self.conn = duckdb.connect(database=config["fts_duckdb_database_name"])

    def __create_schema_in_backend(self, table_name):
        # FIXME: pull this schema from config file
        query = "CREATE TABLE {}(profile_id BIGINT, dbName VARCHAR, path VARCHAR, " \
                "sourceName VARCHAR, columnName VARCHAR, data VARCHAR);".format(table_name)
        self.conn.execute(query)

    def __create_fts_index(self, table_name, index_column):
        # Create fts index over all, *, attributes
        query = "PRAGMA create_fts_index('{}', '{}', '*', stopwords='english')".format(
            table_name, index_column)
        self.conn.execute(query)

        prepare_query = """
            PREPARE fts_query AS (
                WITH scored_docs AS (
                    SELECT *, fts_main_documents.match_bm25(profile_id, ?) AS score FROM documents)
                SELECT profile_id, score
                FROM scored_docs
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT 100)
            """
        self.conn.execute(prepare_query)

    def initialize(self, config):

        self.table_name = config["fts_data_table_name"]
        self.index_column = config["fts_index_column"]

        # FIXME: check if we need to create it or not
        create_schema = True
        if create_schema:
            self.__create_schema_in_backend(self.table_name)

        # create fts index on index_column
        self.__create_fts_index(self.table_name, self.index_column)

    def insert(self, profile_id, dbName, path, sourceName, columnName, data):
        # prepare query and insert
        query = "INSERT INTO {} VALUES ({}, '{}', '{}', '{}', '{}', '{}');".format(
            self.table_name, profile_id, dbName, path, sourceName, columnName, data)

        self.conn.execute(query)

    def fts_query(self, keyword) -> List:
        res = self.conn.execute("EXECUTE fts_query('" + keyword + "')")
        return res.fetchall()
