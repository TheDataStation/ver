# Ver: View Discovery in the Wild

## Setup
If you want to deploy this project locally, please follow the steps below.

Ubuntu18.04 Test Passed

### Install Java 8
```shell
$ sudo apt update
$ sudo apt install openjdk-8-jdk openjdk-8-jre
```

### Build dd_profiler
```shell
$ cd ddprofiler
$ bash build.sh
```

### Setup Elasticsearch
Download Elasticsearch6.0.0

https://www.elastic.co/downloads/past-releases/elasticsearch-6-0-0

Run elasticsearch and then we will run dd_profiler to ingest data to elasticsearch
```shell
$ ./bin/elasticsearch
```

### Run ddprofiler
```shell
$ vim ver/ddprofiler/src/main/resources/datasource_config.yml # edit data source configuration
$ bash run.sh --sources <path_to_sources.yml>
```

## Build Graph Models
After ingesting data into elasticsearch, the next step is to run `networkbuildercoordinator.py` to build a graph on top of data.

In a vanilla linux (debian-based) system, the following packages will need to be installed system-wide:
```shell
$ sudo apt-get install \
     pkg-config libpng-dev libfreetype6-dev `#(requirement of matplotlib)` \
     libblas-dev liblapack-dev `#(speeding up linear algebra operations)` \
     lib32ncurses5-dev
```
Now install requirements for running `networkbuildercoordinator.py` (Requires Python 3 (tested with 3.4, 3.5 and 3.6))
```shell
# create virtual environment
$ pip install virtualenv
$ virtualenv --no-site-packages venv
$ source venv/bin/activate 
$ pip3 install --no-deps -r requirements.txt
```

Add this project to python path
```shell
$ export PYTHONPATH="${PYTHONPATH}:/home/cc/ver"
```

Generate graph models

The model builder is executed from `networkbuildercoordinator.py`, which takes exactly two parameter, --opath, that expects a path to an existing folder where you want to store the built model (in the form of Python pickle files); --tpath, that expects a path to your csv files.

For example:

```shell
$ python networkbuildercoordinator.py --opath test/testmodel/network --tpath table_path
```
Once the model is built, it will be serialized and stored in the provided path.

## Run Example

```
python3 examples/example.py
```