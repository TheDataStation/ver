import duckdb

from typing import List
from dindex_store.common import FullTextSearchIndex


class FTSIndexDuckDB(FullTextSearchIndex):

    def __init__(self, config):
        # FIXME: Validate Config Name
        self.config = config
        self.conn = duckdb.connect(database=config["duckdb_database"])

    def create_fts_index(self, table_name, index_column):
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

    def fts_query(self, keyword) -> List:
        res = self.conn.execute("EXECUTE fts_query('" + keyword + "')")
        return res.fetchall()
