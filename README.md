# Ver: View Discovery in the Wild

Ver is a data discovery system that identifies project-join views over large repositories of tables that do not
contain join path information, and even when input queries are
inaccurate. Ver implements a reference architecture to solve both
the technical (scale and search) and human (semantic ambiguity,
navigating a large number of results) problems of view discovery.

## Architecture

Ver implements a reference architecture consisting of the following components:

### Discovery Engine and Index Creation
This component builds indices over pathless table
collections: i) a join path index, which can be approximate; ii)
retrieval indices over table names, values, and attribute names
and column similarity. The indices are available online, via the
Engine’s API to other components.

### View Specification 
Discovery interfaces include spreadsheet-style, keyword search, APIs, natural language, and combinations of these. 
The reference architecture supports these interfaces via this component. 
For QBE-based interfaces, as implemented by Ver, the input is a set of examples, and the output of this stage is 
a set of example attributes and values. 

### Column Selection
This component selects the subset of tables containing user-provided examples. The output of this component is a collection of candidate tables and columns.

### Join Graph Search
Given a set of candidate tables, an input query, and the discovery index providing join paths, 
this component identifies all join graphs that, when materialized, produce candidate PJ-views. 
The main goal of this component is to address the large join path space. 
To materialize candidate PJ-views, this component uses a Materializer, a data processing component with the capacity 
to execute PJ queries.

### View Distillation 
This component computes categories from the candidate PJ-views that include
redundancy and containment in the views, as well as opportunities for unioning views and more. Some categories can be
used to distill/summarize the views. Others are
shared with the downstream component.

### View Presentation 
This component uses different question interfaces to elicit information from
users via data questions. The questions are designed to narrow
down the space until users find the desired view. The component chooses what questions to ask, sequentially,
using a bandit-based approach.


## Setup
If you want to deploy this project locally, please follow the steps below.

The purpose for this setup phase is to build a knowledge graph model for your chosen dataset 
(corresponding to the Discovery Engine and Index Creation component)
, so it can be feed into the downstream component.

(Tested with Ubuntu 18.04)

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
Download Elasticsearch 6.0.0 from [here](https://www.elastic.co/downloads/past-releases/elasticsearch-6-0-0)

Start elasticsearch
```shell
$ ./bin/elasticsearch
```

### Run ddprofiler

Ingest data to elasticsearch

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
$ export PYTHONPATH="${PYTHONPATH}:/path/to/ver"
```

Generate graph models

The model builder is executed from `networkbuildercoordinator.py`, which takes exactly two parameter, 
--opath, that expects a path to an existing folder where you want to store the built model (in the form of Python pickle files); 
--tpath, that expects a path to your csv files.

For example:

```shell
$ python networkbuildercoordinator.py --opath test/testmodel/network --tpath table_path
```
Once the model is built, it will be serialized and stored in the provided path.

## Run Example

Once you have the graph model, you can try running the end-to-end pipeline in `examples/example.py`

Before you run, there are several parameters you need to set in the example file:

```python
model_path = ''  # path to the graph model
base_path = ''  # path to the source csv tables
output_path = ''  # path to output the views
sep = ','  # seperator used for csv files
vd_results_dir = "" # result path for view distillation
```

Once you modify these parameters, run

```
python3 examples/example.py
```

The View Presentation component will use the output of View Distillation component,
which is stored in `vd_results_dir`, to ask questions and interact with the user until the user finds a view.

## Future Works

This project is currently a work in progress, and we expect to have all documentation, test and a full demo 
done by the 2nd quarter of 2023.