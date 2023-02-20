from dindex_store.common import FullTextSearchIndex

import duckdb


class FTSIndexDuckDB(FullTextSearchIndex):

    def __init__(self):
        # FIXME: take DB name from config file
        self.con = duckdb.connect()

    def query(self, kw):
        kw = "household"
        res = self.con.execute("EXECUTE fts_query('" + kw + "')")
        results = res.fetchall()
        return results


if __name__ == "__main__":
    print("FTS index implementation on DuckDB")
