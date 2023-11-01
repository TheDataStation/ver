import os
import yaml
from pathlib import Path
from typing import List, Dict

import duckdb
from duckdb import BinderException, CastException, CatalogException, ConnectionException, ConstraintException, DataError, ConversionException

from dindex_store.common import ProfileIndex


class ProfileIndexDuckDB(ProfileIndex):

    def __init__(self, config: Dict, load=False, force=False) -> None:
        ProfileIndexDuckDB._validate_config(config)
        self.config = config
        db_path = Path(config['ver_base_path']) / Path(config['profile_duckdb_database_name'])
        self.conn = duckdb.connect(database=str(db_path))
        self.schema = ""
        self._field_order = []

        if not load:
            profile_table_name = config["profile_table_name"]
            profile_schema_path = Path(config['ver_base_path'] / config['unified_profile_schema_name']).absolute()
            self.schema = self._read_unified_profile_schema(profile_table_name, profile_schema_path)
            try:
                if force:
                    profile_table_name = config["profile_table_name"]
                    q = f"DROP TABLE IF EXISTS {profile_table_name};"
                    self.conn.execute(q)
                for statement in self.schema.split(";"):
                    self.conn.execute(statement)
            except:
                print("An error has occurred when reading the schema")
                raise

    def add_profile(self, node: Dict) -> bool:
        try:
            profile_table = self.conn.table(self.config["profile_table_name"])
            to_insert = [node[field] if field in node else None for field in self.field_order]
            profile_table.insert(to_insert)
            return True
        except BinderException as be:
            print(f"An error has occured when trying to add profile: {be}")
            return False
        except CastException as ce:
            print(f"An error has occured when trying to add profile: {ce}")
            return False
        except CatalogException as cae:
            print(f"An error has occured when trying to add profile: {cae}")
            return False
        except ConnectionException as coe:
            print(f"An error has occured when trying to add profile: {coe}")
            return False
        except ConstraintException as cone:
            print(f"An error has occured when trying to add profile: {cone}")
            return False
        except ConversionException as conve:
            print(f"An error has occured when trying to add profile: {conve}")
            return False
        except DataError as de:
            print(f"An error has occured when trying to add profile: {de}")
            return False

    def get_filtered_profiles_from_table(self, table_name, desired_attributes: List[str]):
        profile_table = self.config["profile_table_name"]
        project_list = ",".join(desired_attributes)
        try:
            query = f"SELECT {project_list} FROM {profile_table} WHERE sourcename = '{table_name}'"
            result = self.conn.execute(query)
            return result.fetchall()
        except BinderException as be:
            print(f"""An error has occured when trying to get profile: {be}""")
            return False

    def get_filtered_profiles_from_nids(self, nids, desired_attributes: List[str]):
        project_list = ",".join(desired_attributes)
        predicate = "OR id = ".join([str(n) for n in nids])
        try:
            profile_table = self.config["profile_table_name"]
            query = f"""SELECT {project_list} FROM {profile_table} WHERE id = {predicate}"""
            results = self.conn.execute(query)
            return results.fetchall()
        except BinderException as be:
            print(f"""An error has occured when trying to get profile {be}""")
            return False
        
    def get_profile(self, node_id: int) -> Dict:
        pass
    
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
        assert "unified_profile_schema_name" in config, "Error: schema_path is missing"
        profile_schema_path = Path(config['ver_base_path'] / config['unified_profile_schema_name']).absolute()
        if not os.path.isfile(profile_schema_path):
            raise ValueError("The path does not exist, or is not a file")
        
        assert "profile_table_name" in config

    @classmethod
    def _read_unified_profile_schema(self, profile_table_name, path) -> str:
        field_order = []
        with open(path, "r") as stream:
            try:
                file_load = yaml.safe_load(stream)
                schema = f"CREATE TABLE {profile_table_name} (\n"

                for attribute in file_load['attributes']:
                    schema += f"    {attribute['name']} {attribute['type']},\n"
                    field_order.append(attribute['name'])
                for analyzer in file_load['analyzers']:
                    if not analyzer['enabled']:
                        continue
                    for field in analyzer['fields']:
                        schema += f"    {field['name']} {field['type']},\n"
                        field_order.append(field['name'])

                self.field_order = field_order
                schema = schema[:-2] + ")"
                return schema
            except yaml.YAMLError as e:
                print(f"""An error has occured when trying to parse profile schema file: {e}""")
                return None
            
    # Field order is used to ensure that the order in the schema is the same as the order in insert
    @property
    def field_order(self):
        return self._field_order
    
    @field_order.setter
    def field_order(self, value):
        self._field_order = value


if __name__ == "__main__":
    print("ProfileIndexDuckDB")

    import config

    cnf = dict(config)
    index = ProfileIndexDuckDB(cnf)

    # FIXME: what to do if this file is invoked individually?
