#! /usr/bin/env python3

import subprocess
from dataclasses import dataclass
from os import environ
from pathlib import Path
from warnings import warn
# from knowledgerepr.ekgstore.neo4j_store import Neo4jExporter
from fire import Fire
import IPython
from main import init_system

run_cmd = subprocess.call

get_env = environ.get

# Global, Final Variables
AURUM_SRC_HOME_ENV = "AURUM_SRC_HOME"
DDPROFILER_NAME = "ddprofiler"
DDPROFILER_RUN = "run.sh"
AURUM_HOME_ENV = "AURUM_HOME"
SOURCES_FILE_STARTING_TEMPLATE = """#######
                        # This file is specified as input to the ddprofiler, which uses it to extract a list of
                        # sources that are necessary to process and profile.
                        #
                        # This file consists of an attribute 'sources' which is a list of
                        # data sources, that describe the different sources that need to be processed.
                        # Each source has three mandatory attributes, 'name', which will be
                        # used to refer to the source and should be descriptive. 'type', which indicates what
                        # type of source we are dealing with, one of [csv, postgres, mysql, oracle10g, oracle11g].
                        # Finally, a 'config' object, which will contain source specific properties.
                        #
                        # Each type of source has a different set of properties that are necessary to configure it.
                        # These are all part of the 'config' object, that each source contains.
                        # For example, a folder in a filesystem will require a path, while a database server will
                        # require credentials to access it. We document the properties required by each source below.
                        #######
                        
                        api_version: 0
                        
                        # In sources we include as many data sources as desired
                        sources:
                        
                        # Include a source for each data source to configure"""


class BaseAurumException(Exception):
    pass


class DataSourceNotConfigured(BaseAurumException):
    pass


class ModelNotFoundError(BaseAurumException):
    pass


@dataclass()
class CSVDataSource:
    name: str = 'NO NAME PROVIDED'
    _path: Path = Path.cwd()
    separator: str = ','

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, p):
        assert isinstance(p, Path) or isinstance(p, str)
        self._path = Path(p)

    def __dict__(self):
        return {
            'name': self.name,
            'type': 'csv',
            'config': {
                'path': str(self.path),
                'separator': self.separator
            }
        }

    def to_yml(self):
        return f"""api_version: 0
                    sources:
                    - name: "{self.name}"
                      type: csv
                      config:
                        path: "{str(self.path)}"
                        separator: '{self.separator}'"""


@dataclass()
class DBDataSource:
    name: str = ''
    host: str = ''
    _port: int = ''
    db_name: str = ''
    db_user: str = ''
    db_password: str = ''
    _type: str = ''

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, p):
        self._port = int(p)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, s):
        self._type = s

    def __dict__(self):
        return {
            'name': self.name,
            'type': self.type,
            'config': {
                'db_server_ip': self.host,
                'db_server_port': self.port,
                'database_name': self.db_name,
                'db_username': self.db_user,
                'db_password': self.db_password
            }
        }

    def to_yml(self):
        return f"""api_version: 0
                    sources:
                    - name: "{self.name}"
                      type: {self.type}
                      config:
                        db_server_ip: {self.host}
                        db_server_port: {self.port}
                        database_name: {self.db_name}
                        db_username: {self.db_user}
                        db_password: {self.db_password}"""


class AurumWrapper(object):
    """
    Class that acts as a bridge between the current Aurum source code and an higher-level APIs (e.g. CLI and Web).
    Relies heavily on filesystem CRUD interaction (e.g. relative paths) and thus should be removed once imports can be handled at the code level.
    Eventually the FS will need to be replaced with something like SQLite to store data sources, track profilings etc.
    """

    def __init__(self):
        self.aurum_src_home = Path(get_env(AURUM_SRC_HOME_ENV, Path.cwd()))
        self.ddprofiler_home = self.aurum_src_home.joinpath(DDPROFILER_NAME)
        self.ddprofiler_run_sh = self.ddprofiler_home.joinpath(DDPROFILER_RUN)
        self.aurum_home = Path(get_env(AURUM_HOME_ENV, Path.home().joinpath('.aurum')))
        try:
            self.aurum_home.mkdir(parents=True)
        except FileExistsError:
            pass

        self.sources_dir = self.aurum_home.joinpath('sources')
        try:
            self.sources_dir.mkdir(parents=True)
        except FileExistsError:
            pass

        self.models_dir = self.aurum_home.joinpath('models')
        try:
            self.models_dir.mkdir(parents=True)
        except FileExistsError:
            pass

    def _make_data_source_path(self, ds_name):
        return self.sources_dir.joinpath(ds_name + '.yml')

    def _make_model_path(self, model_name):
        return self.models_dir.joinpath(model_name)

    @property
    def sources(self):
        return [f.name.replace('.yml', '') for f in self.sources_dir.iterdir()]

    @property
    def models(self):
        return [f.name for f in self.models_dir.iterdir()]

    def _make_csv_data_source(self, name, fp, separator=','):
        return {
            'name': name,
            'type': 'csv',
            'config': {
                'path': fp,
                'separator': separator
            }
        }

    def _store_data_source(self, ds):
        with open(self._make_data_source_path(ds.name), 'w') as f:
            f.write(ds.to_yml())


class AurumCLI(AurumWrapper):

    def __init__(self):
        super().__init__()

    # ----------------------------------------------------------------------
    # Configure sources functions

    def show_sources(self, source_path):
        """
        Shows every source configured in the input source_path YAML file
        :param source_path: the input source.yml file
        :return: prints the list of configured sources
        """
        with open(source_path) as f:
            print(f.read())

    def create_sources(self, sources_name) -> bool:
        """
        Creates a new sources file to be configured with sources
        :param sources_name: the name of the sources file
        :return: boolean
        """
        with open(self._make_data_source_path(sources_name), 'w') as f:
            f.write(SOURCES_FILE_STARTING_TEMPLATE)

    @property
    def sources(self):
        return super().sources

    def add_csv_data_source(self, name, path, sep=','):
        ds = CSVDataSource()
        ds.name = name
        ds.path = path
        ds.separator = sep
        super()._store_data_source(ds)

    def add_db_data_source(self, name, db_type, host, port, db_name, username, password):
        # TODO check if `db_type` is supported
        ds = DBDataSource()
        ds.name = name
        ds.type = db_type
        ds.host = host
        ds.port = port
        ds.db_name = db_name
        ds.db_user = username
        ds.db_password = password
        super()._store_data_source(ds)

    # ----------------------------------------------------------------------
    # Profile Functions

    def profile(self, data_source_name):
        ds_fp = super()._make_data_source_path(data_source_name)
        if not ds_fp.exists():
            raise DataSourceNotConfigured(f"Data Source {data_source_name} not configured!")
        profile_cmd = ['bash', self.ddprofiler_run_sh, '--sources', ds_fp]
        run_cmd(profile_cmd, cwd=self.ddprofiler_home)

    # ----------------------------------------------------------------------
    # DIndex Functions

    def build_model(self, name):
        model_dir_path = self._make_model_path(name)
        try:
            model_dir_path.mkdir(parents=True)
        except FileExistsError:
            warn(f'Model with the same name ({name}) already exists!')

        run_cmd(['python', 'networkbuildercoordinator.py', '--opath', model_dir_path])

    # def export_model(self, model_name, to='neo4j', neo4j_host='localhost', neo4j_port=7687, neo4j_user='neo4j',
    #                  neo4j_pass='n304j'):
    #     supported_destionations = ['neo4j']
    #
    #     if to not in supported_destionations:
    #         raise NotImplementedError(f"Model destination not supported. Only {supported_destionations} are supported")
    #
    #     model_dir_path = self._make_model_path(model_name)
    #
    #     # Check if model exists
    #     # TODO refactor to separate method
    #     if not model_dir_path.exists():
    #         available_models = '\n'.join(self.models)
    #         raise ModelNotFoundError(
    #             f"Model {model_name} not found!\nHere are the available ones:\n{available_models}")
    #
    #     # Hacky way. The underlying `fieldnetwork.py:deserialize_network` should be changed
    #     model_path_str = model_dir_path.__str__() + '/'
    #     if to == 'neo4j':
    #         exporter = Neo4jExporter(host=neo4j_host, port=neo4j_port, user=neo4j_user, pwd=neo4j_pass)
    #     exporter.export(model_path_str)

    def clear_store(self):
        """
        γφ
        """
        from elasticsearch import Elasticsearch
        # TODO extract AURUM_ES_HOST
        es = Elasticsearch()
        es.indices.delete('profile')
        es.indices.delete('text')

    def explore_model(self, model_name):
        """
        Initiates an interactive IPython session to run discovery queries.

        :param model_name:
        :return:
        """
        api, reporting = init_system(self._make_model_path(model_name).__str__() + '/', create_reporting=True)
        IPython.embed()


if __name__ == '__main__':
    aurum_cli = AurumCLI()
    Fire({
        'show-sources': aurum_cli.show_sources,
        'create-sources': aurum_cli.create_sources,
        'add-csv': aurum_cli.add_csv_data_source,
        'add-db': aurum_cli.add_db_data_source,
        'list-sources': aurum_cli.sources,


        'profile': aurum_cli.profile,

        'build-model': aurum_cli.build_model,
        'list-models': aurum_cli.models,
        'export-model': aurum_cli.export_model,
        'clear-store': aurum_cli.clear_store,

        'explore-model': aurum_cli.explore_model
    })
