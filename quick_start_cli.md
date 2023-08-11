# Quick Start
## Setup
Compatible with Python versions 3.8.10 and higher
### Install dependencies
```bash
# Create an virtual environment 
# (optional but recommended)
$ python -m venv venv 
$ source venv/bin/activate
# install dependencies
(venv) $ pip install -r requirements_quick_start.txt
```

## Create sources config file
Before running Ver you must prepare a config file that contains the information about the data sources. The config file is in yaml format.
### Create the file
First you need to create an empty config file that later will be filled with the information about the data sources. The following command is how to create sources file with name `SOURCE_FILE_NAME`.
The source file will be stored under `.dsessions/sources/`

```bash
python ver_cli.py create_sources_file SOURCE_FILE_NAME
```

### Add csv files to the sources list
After creating the empty config file, the next step is to add the folder containing the csv files to the sources list. The following command is how to add a csv file to the sources list.
```bash
python ver_cli.py add_csv SOURCE_FILE_NAME CSV_SOURCE_NAME PATH_TO_CSV_FOLDER [--sep DELIMITER]
```
- `SOURCE_FILE_NAME` is the name of the sources file created in the previous step.
- `CSV_SOURCE_NAME` is the name of the csv source.
- `PATH_TO_CSV_FOLDER` is the path to the folder containing the csv files.
- `DELIMITER` is the delimiter used in the csv files. Default is `,`.

## Build Data Profiles
After entering all the data sources, the next step is to profile the data. The following command is how to build data profiles, use `--build` flag to (re)build the data profiler.
```bash
python ver_cli.py profile SOURCE_FILE_NAME OUTPUT_PROFILE_PATH [--build] [--store_type STORE_TYPE]
```
- `SOURCE_FILE_NAME` is the name of the sources file created in the previous step.
- `OUTPUT_PROFILE_PATH` is the path to the folder where the data profiles will be stored.
- `STORE_TYPE` is the type of storage to use for the data profiles. Default is 3 (json files with text CSV for full text search).

## Build discovery indices on top of data profiles
After building data profiles, the next step is to build discovery index by using the following command.
```bash
python ver_cli.py build_dindex PROFILE_PATH [--force]
```
- `PROFILE_PATH` is the path to the folder where the data profiles are stored.
- `--force` is an optional flag to force the re-building of the discovery index.

## Run Ver setup on the demo dataset
This code below is an example on how to run Ver setup on the demo dataset.
```bash
python ver_cli.py create_sources_file quickstart
python ver_cli.py add_csv quickstart quickstart demo_dataset/
python ver_cli.py profile quickstart output_profiles_json
python ver_cli.py build_dindex output_profiles_json/ --force
```

## Run Ver end-to-end
```bash
python ver_quickstart.py
```

In `ver_quickstart.py`, we give an end-to-end example of how to use the query-by-example module of Ver.


The output is a set of materialized views (`output/view{i}.csv`) and each view is associated with a metadata profile (`output/view{i}.json`). `view{i}.json` has two fields: `join_graph` and `columns_proj`. `join_graph` is the join graph that is used to generate the view and `columns_proj` are the columns projected from the view.