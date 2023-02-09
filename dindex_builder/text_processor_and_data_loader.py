from os import listdir
from os.path import isfile, join
import csv

import duckdb
from tqdm import tqdm

CSV_DELIMITER = ","


def create_schema_in_backend(con, table_name):
    # FIXME: pull this schema from config file
    query = "CREATE TABLE {}(profile_id BIGINT, dbName VARCHAR, path VARCHAR, " \
            "sourceName VARCHAR, columnName VARCHAR, data VARCHAR);".format(table_name)
    con.execute(query)


def load_csv(con, table_name, csv_file_path):
    with open(csv_file_path) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=CSV_DELIMITER)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
                continue
            profile_id = int(row[0])
            dbName = row[1]
            path = row[2]
            sourceName = row[3]
            columnName = row[4]
            data = row[5]
            # prepare query and insert
            query = "INSERT INTO {} VALUES ({}, '{}', '{}', '{}', '{}', '{}');".format(
                table_name, profile_id, dbName, path, sourceName, columnName, data)
            con.execute(query)


def load_data(con, table_name, path, create_schema=True):
    # create schema
    if create_schema:
        create_schema_in_backend(con, table_name)
    # enumerate files in path
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    for file in tqdm(onlyfiles):
        load_csv(con, table_name, file)


def create_fts_index(con, table_name, index_column):
    # create fts index over all, *, attributes
    query = "PRAGMA create_fts_index('{}', '{}', '*', stopwords='english')".format(table_name, index_column)
    con.execute(query)

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
    con.execute(prepare_query)


if __name__ == "__main__":
    print("Text processor and data loader ")

    # config file
    path = "/Users/raulcf/repos/projects/ver/ddprofiler/output_profiles_json_2023-02-07T09-59-14-369-06\:00/text/"
    table_name = "documents"
    index_column = "profile_id"

    # create connection to duckdb
    # FIXME: give path to duckdb here
    con = duckdb.connect()

    # load data into duckdb
    load_data(con, table_name, path)

    # create FTS index on duckdb
    create_fts_index(con, table_name, index_column)
