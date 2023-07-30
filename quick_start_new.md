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

## Build Data Profiles
Config data sources in ver/.dsessions/sources/data_config.yml

```bash
python ver_cli.py profile data_config output_profiles_json --store_type 3
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