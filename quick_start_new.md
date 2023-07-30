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

## Create config file
```bash
python ver_cli.py create_sources_file quickstart
python ver_cli.py add_csv quickstart quickstart <DIR>
```
`<DIR>` is an absolute path to your dataset.

## Build Data Profiles
```bash
python ver_cli.py profile quickstart output_profiles_json --store_type 3
```
Data profiles are stored as json files in ddprofiler/output_profiles_json

## Build discovery indices on top of data profiles
```bash
python ver_cli.py build_dindex ddprofiler/output_profiles_json/ --force
```

## Run Ver on the demo dataset
```bash
python ver_test.py
```