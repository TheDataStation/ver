#! /usr/bin/env python3

from dataclasses import dataclass
import os
import subprocess
from pathlib import Path
from textwrap import dedent

from fire import Fire
from aurum_api import algebra


class BaseAurumException(Exception):
    pass


class DataSourceError(BaseAurumException):
    pass


class DIndexConfigurationError(BaseAurumException):
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
        return f"""\
                - name: "{self.name}"
                  type: csv
                  config:
                    path: "{str(self.path)}"
                    separator: '{self.separator}'
                """
    
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
        return f"""\
                - name: "{self.name}"
                  type: {self.type}
                  config:
                    db_server_ip: {self.host}
                    db_server_port: {self.port}
                    database_name: {self.db_name}
                    db_username: {self.db_user}
                    db_password: {self.db_password}
                """


class VerCLI:
    """
    Class that acts as a bridge between the current Aurum(Ver) source code and an higher-level APIs (e.g. CLI and Web).
    Relies heavily on filesystem CRUD interaction (e.g. relative paths) and thus should be removed once imports can be handled at the code level.
    Eventually the FS will need to be replaced with something like SQLite to store data sources, track profilings etc.
    """

    def __init__(self):
        # Take the value for VER_HOME from the environment if available, or current directory by default
        self.VER_HOME = os.environ.get("VER_HOME", Path.cwd())

        # self.AURUM_SRC_HOME_ENV = "AURUM_SRC_HOME"
        self.DDPROFILER_NAME = "ddprofiler"
        self.DDPROFILER_BUILD = "build.sh"
        self.DDPROFILER_RUN = "run.sh"
        self.DDPROFILER_JSON_OUTPUT = "/json/"
        self.DDPROFILER_TEXT_OUTPUT = "/text/"
        self.VER_DISCOVERY_SESSIONS_PATH = "VER_DISCOVERY_SESSIONS_PATH"

        self.ver_home = Path(self.VER_HOME)
        self.ddprofiler_home = self.ver_home.joinpath(self.DDPROFILER_NAME)
        self.ddprofiler_build_sh = self.ddprofiler_home.joinpath(self.DDPROFILER_BUILD)
        self.ddprofiler_run_sh = self.ddprofiler_home.joinpath(self.DDPROFILER_RUN)
        self.discovery_sessions_dir = Path(os.environ.get(self.VER_DISCOVERY_SESSIONS_PATH,
                                                          Path.cwd().joinpath('.dsessions')))
        try:
            self.discovery_sessions_dir.mkdir(parents=True)
        except FileExistsError:
            pass

        self.sources_dir = self.discovery_sessions_dir.joinpath('sources')
        try:
            self.sources_dir.mkdir(parents=True)
        except FileExistsError:
            pass

        self.SOURCES_FILE_STARTING_TEMPLATE = """\
                                        #######
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

                                        # Include a source for each data source to configure
                                        """

    def _make_data_source_path(self, ds_name):
        # FIXME: if the file already exists, then raise exception and ask for a different name
        return self.sources_dir.joinpath(ds_name + '.yml')

    @property
    def list_sources_files(self):
        return [f.name.replace('.yml', '') for f in self.sources_dir.iterdir()]

    def _get_source_path(self, source_name):
        name = source_name
        if not source_name[-4:] == ".yml":
            name = source_name + ".yml"
        return self.sources_dir.joinpath(name)

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

    # ----------------------------------------------------------------------
    # Configure sources functions

    def inspect_sources_file(self, source_name):
        """
        Shows every source configured in the input source_path YAML file
        :param source_name: the name of the sources file
        :return: prints the list of configured sources in source_name
        """
        path = self._get_source_path(source_name)
        with open(path) as f:
            print(f.read())

    def create_sources_file(self, sources_file_name) -> bool:
        """
        Creates a new sources file to be configured with sources
        :param sources_file_name: the name of the sources file
        :return: boolean
        """
        path = self._make_data_source_path(sources_file_name)
        if os.path.exists(path):
            print("Error: File {} already exists".format(path))
            return False
        with open(path, 'w') as f:
            f.write(dedent(self.SOURCES_FILE_STARTING_TEMPLATE))
        return True

    def add_csv(self, sources_file_name, csv_source_name, path_to_csv_files, sep=',') -> bool:
        ds = CSVDataSource()
        ds.name = csv_source_name
        if Path(path_to_csv_files).is_absolute():
            ds.path = path_to_csv_files
        else:
            ds.path = Path.cwd().joinpath(path_to_csv_files)
        ds.separator = sep

        # super()._store_data_source(ds)
        # Verify the sources_file_name exist
        path = self._make_data_source_path(sources_file_name)
        if not os.path.exists(path):
            raise DataSourceError("Error: Sources file {} does not exist, you need to create a sources file before"
                                  " adding datasets.".format(path))
        # Append the source to the file
        with open(path, 'a') as f:
            yaml_data = dedent(ds.to_yml())
            f.write(yaml_data)
        return True

    def add_database(self, name, db_type, host, port, db_name, username, password):
        # TODO check if `db_type` is supported
        # TODO give better names to the below variables
        ds = DBDataSource()
        ds.name = name
        ds.type = db_type
        ds.host = host
        ds.port = port
        ds.db_name = db_name
        ds.db_user = username
        ds.db_password = password

        # super()._store_data_source(ds)
        # TODO: follow add_csv_data_source to complete this function

    # ----------------------------------------------------------------------
    # Profile Functions

    def profile(self, sources_file_name, output_path, store_type: int=3, build=False):
        path = self._make_data_source_path(sources_file_name)
        if not path.exists():
            raise DataSourceError(f"Data Source {sources_file_name} not configured!")
        if not Path(output_path).is_absolute():
            output_path = Path.cwd().joinpath(output_path)
        if build:
            build_cmd = ['bash', self.ddprofiler_build_sh,]
            subprocess.call(build_cmd, cwd=self.ddprofiler_home)
        profile_cmd = ['bash', self.ddprofiler_run_sh, '--sources', path, '--store.json.output.folder', output_path,
                       '--store.type', str(store_type)]
        subprocess.call(profile_cmd, cwd=self.ddprofiler_home)

    # ----------------------------------------------------------------------
    # DIndex Functions

    def build_dindex(self, profile_data_path, force=False):
        # try:
        #     p = Path(output_dindex_path)
        #     p.mkdir(parents=True)
        # except FileExistsError:
        #     # warn(f'Model with the same name ({output_dindex_path}) already exists!')
        #     raise DIndexConfigurationError(f"path {output_dindex_path} already exists!")
        from dindex_builder import dindex_builder
        import config
        cnf = {setting: getattr(config, setting) for setting in dir(config) if setting.islower()}
        # TODO: provide alternative way of configuring dindex build (other than config)
        dindex_builder.build_dindex(profile_data_path, cnf, force)
        # subprocess.call(['python', 'dindex_builder/dindex_builder.py', 'build', '--input_path', input_data_path])

    def load_dindex(self):
        from dindex_store.discovery_index import load_dindex
        import config
        cnf = {setting: getattr(config, setting) for setting in dir(config) if setting.islower()}
        # TODO: provide alternative way of configuring dindex build (other than config)
        load_dindex(cnf)

        # subprocess.call(['python', 'build_dindex.py', 'load'])

    def start_aurum_api_session(self, dindex_path):
        """
        Initiates an interactive IPython session to run discovery queries.

        :param model_name:
        :return:
        """
        import IPython
        # interactive false, as it's already gonna be embedded here, and no reporting needed
        api, reporting = algebra.main(dindex_path, interactive=False, create_reporting=False)
        # api, reporting = init_system(dindex_path + '/', create_reporting=True)
        IPython.embed()


if __name__ == '__main__':
    ver = VerCLI()
    Fire({
        'create_sources_file': ver.create_sources_file,
        'list_sources_files': ver.list_sources_files,
        'inspect_sources_file': ver.inspect_sources_file,
        'add_csv': ver.add_csv,
        'add_database': ver.add_database,

        'profile': ver.profile,

        'build_dindex': ver.build_dindex,
        'load_dindex': ver.build_dindex,

        'aurum_api': ver.start_aurum_api_session

        # TODO: include qbe, distillation, presentation
    })
