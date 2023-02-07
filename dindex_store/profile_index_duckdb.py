import os
import duckdb

from typing import Dict

from dindex_store.discovery_index import ProfileIndex


class ProfileIndexDuckDB(ProfileIndex):

    def __init__(self, config: Dict) -> None:
        ProfileIndexDuckDB._validate_config(config)
        self.config = config
        self.conn = duckdb.connect()
        self.schema = ""

        with open(config["profile_schema_path"]) as f:
            self.schema = f.read()
        try:
            for statement in self.schema.split(";"):
                self.conn.execute(statement)
        except:
            print("An error has occurred when reading the schema")
            raise

    def add_profile(self, node: Dict) -> bool:
        if "minhash" in node and node["minhash"]:
            node["minhash"] = ",".join(map(str, node["minhash"]))
        
        try:
            profile_table = self.conn.table(self.config["profile_table_name"])
            profile_table.insert(node.values())
            return True
        except:
            print("An error has occured when trying to add profile")
            return False

    def get_profile(self, node_id: int) -> Dict:
        try:
            profile_table = self.config["profile_table_name"]
            result = self.conn.execute(
                f"SELECT * FROM {profile_table} WHERE id = {node_id}") \
                .to_dict(orient='records')[0]
            return True
        except:
            print("An error has occured when trying to get profile")
            return False

    @classmethod
    def _validate_config(cls, config):
        assert "profile_schema_path" in config, "Error: schema_path is missing"
        if not os.path.isfile(config["profile_schema_path"]):
            raise ValueError("The path does not exist, or is not a file")
        
        assert "profile_table_name" in config
