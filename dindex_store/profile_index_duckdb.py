import os
import duckdb

from typing import Dict

from dindex_store.common import ProfileIndex


class ProfileIndexDuckDB(ProfileIndex):

    def __init__(self, config: Dict, load=False) -> None:
        ProfileIndexDuckDB._validate_config(config)
        self.config = config
        self.conn = duckdb.connect(database=config["profile_duckdb_database_name"])
        self.schema = ""

        if not load:
            with open(config["profile_schema_path"]) as f:
                self.schema = f.read()
            try:
                for statement in self.schema.split(";"):
                    self.conn.execute(statement)
            except:
                print("An error has occurred when reading the schema")
                raise

    def initialize(self, config):
        # TODO: index creation, others
        return

    def add_profile(self, node: Dict) -> bool:
        # if "minhash" in node and node["minhash"]:
        #     node["minhash"] = ",".join(map(str, node["minhash"]))

        try:
            profile_table = self.conn.table(self.config["profile_table_name"])
            profile_table.insert(node.values())
            return True
        except:
            print("An error has occured when trying to add profile")
            return False

    def get_profile(self, node_id: int) -> Dict:
        profile_table = self.config["profile_table_name"]
        try:
            result = self.conn.execute(
                f"SELECT * FROM {profile_table} WHERE id = {node_id}") \
                .to_dict(orient='records')[0]
            return True
        except:
            print("An error has occured when trying to get profile")
            return False

    def get_filtered_profiles_from_table(self, table_name, desired_attributes):
        profile_table = self.config["profile_table_name"]
        project_list = ",".join(desired_attributes)
        try:
            result = self.conn.execute(
                f"SELECT {project_list} FROM {profile_table} WHERE s_name = {table_name}") \
                .to_dict(orient='records')[0]
            return result
        except:
            print("An error has occured when trying to get profile")
            return False

    def get_filtered_profiles_from_nids(self, nids, desired_attributes):
        project_list = ",".join(desired_attributes)
        predicate = "OR id = ".join(nids)
        try:
            profile_table = self.config["profile_table_name"]
            result = self.conn.execute(
                f"SELECT {project_list} FROM {profile_table} WHERE id = {predicate}") \
                .to_dict(orient='records')[0]
            return result
        except:
            print("An error has occured when trying to get profile")
            return False

    def get_minhashes(self) -> Dict:
        # Get all profiles with minhash
        profile_table = self.conn.table(self.config["profile_table_name"])
        return profile_table.filter('minhash IS NOT NULL') \
            .project('id, decode(minhash)') \
            .to_df() \
            .rename(columns={'decode(minhash)': 'minhash'}) \
            .to_dict(orient='records')

    @classmethod
    def _validate_config(cls, config):
        assert "profile_schema_path" in config, "Error: schema_path is missing"
        if not os.path.isfile(config["profile_schema_path"]):
            raise ValueError("The path does not exist, or is not a file")
        
        assert "profile_table_name" in config


if __name__ == "__main__":
    print("ProfileIndexDuckDB")

    import config

    cnf = dict(config)
    index = ProfileIndexDuckDB(cnf)

    # FIXME: what to do if this file is invoked individually?
