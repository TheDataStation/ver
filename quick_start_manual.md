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
# setup environment variables
export PYTHONPATH="${PYTHONPATH}:{your_path}/ver/"
```

## Build Data Profiles
Config data sources in ver/ddprofiler/app/src/main/resources/data_config.yml

Append the following to data_config.yml

```yaml
  - name: "quickstart"
    type: csv
    config:
      # path indicates where the CSV files live
      path: "/Users/yuegong/Documents/ver/demo_dataset/"
      # separator used in the CSV files
      separator: ','
```

```bash
cd ddprofiler
bash run.sh --sources app/src/main/resources/data_config.yml --store_type 1
```
Data profiles are stored as json files in ddprofiler/output_profiles_json

## Build discovery indices on top of data profiles
```bash
cd dindex_builder
python3 dindex_builder.py --profile_data_path ../ddprofiler/output_profiles_json --force
```

## Run Ver on the demo dataset
```bash
python ver_quick_start.py
```