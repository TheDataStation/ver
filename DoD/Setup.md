## Quick Start

```shell
git clone https://github.com/TheDataStation/aurum-dod-staging
cd aurum-dod-staging
```

We explain next how to configure the modules to get a barebones installation. We
do this in a series of 3 stages.

### Stage 1: Configuring DDProfiler

The profiler is built in Java (you can find it under /ddprofiler). The input are
data sources (files and tables) to analyze and the output is stored in
elasticsearch. Next, you can find instructions to build and deploy the profiler as well as
to install and configure Elasticsearch.

#### Building ddprofiler

You will need JVM 8 available in the system for this step. From the root directory go to 'ddprofiler' and do:

Install java8 on linux - [Tutorial](https://www.digitalocean.com/community/tutorials/how-to-install-java-on-centos-and-fedora)
```shell
$> cd ddprofiler
$> bash build.sh 
```

#### Deploying Elasticsearch (tested with 6.0.0)

Download the software (note the currently supported version is 6.0.0) from:

https://www.elastic.co/products/elasticsearch

Uncompress it and then simply run from the root directory:

```shell
$> ./bin/elasticsearch
```

that will start the server in localhost:9200 by default, which is the address
you should use to configure ddprofiler as we show next.

#### Configuration of ddprofiler

There are two different ways of interacting with the profiler. One is through a
YAML file, which describes and configures the different data sources to profile.
The second way is through an interactice interface which we are currently
working on. We describe next the configuration of sources through the YAML file.

The jar file produced in the previous step accepts a number of flags, of which
the most relevant one is:

**--sources** Which accepts a path to a YAML file in which to configure the
access to the different data sources, e.g., folder with CSV files or
JDBC-compatible RDBMS.

You can find an example template file
[here](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/resources/template.yml)
which contains documentation to explain how to use it. 

A typical usage of the profiler from the command line will look like:

Example:

```shell
$> bash run.sh --sources <path_to_sources.yml> 
```

You can consult all configuration parameters by appending **--help** or <?> as a
parameter. In particular you may be interested in changing the default
elasticsearch ports (consult *--store.http.port* and *--store.port*) in case
your installation does not use the default ones.

*Note that, although the YAML file accepts any number of data sources, at the
moment we recommend to profile one single source at a time.* Note, however, that
you can run ddprofiler as many times as necessary using a YAML with a different
data source. For example, if you want to index a repository of CSV files and a
RDBMS, you will need to run ddprofiler two times, each one configured to read
the data from each source. All data summaries will be created and stored in
elasticsearch. Only make sure to edit the YAML file appropriately each time.

### Stage 2: Building a Model

Once you have used the ddprofiler to create data summaries of all the data
sources you want, the second stage will read those and create a model. We
briefly explain next the requirements for running the model builder.

#### Requirements

*As typical with Python deployments, we recommend using a virtualenvironment (see
virtualenv) so that you can quickly wipeout the environment if you no longer
need it without affecting any system-wide dependencies.* 

Requires Python 3 (tested with 3.4, 3.5 and 3.6). Use requirements.txt to
install all the dependencies:

```shell
$> pip3 install --no-deps -r requirements.txt
```

In a vanilla linux (debian-based) system, the following packages will need to be installed system-wide:

```shell
sudo apt-get install \
     pkg-config libpng-dev libfreetype6-dev `#(requirement of matplotlib)` \
     libblas-dev liblapack-dev `#(speeding up linear algebra operations)` \
     lib32ncurses5-dev
```

Some notes for MAC users:

If you run within a virtualenvironemtn, Matplotlib will fail due to a mismatch with the backend it wants to use. A way of fixing this is to create a file: *~/.matplotlib/matplotlibrc* and add a single line: *backend: TkAgg*.

Note you need to use elasticsearch 6.0.0 in the current version.

#### Deployment

The model builder is executed from 'networkbuildercoordinator.py', which takes
exactly two parameter, **--opath**, that expects a path to an existing folder
where you want to store the built model (in the form of Python pickle files); **--tpath**,
that expects a path to your csv files.

For example:

```shell
$> python networkbuildercoordinator.py --opath test/testmodel/network --tpath table_path
```

Once the model is built, it will be serialized and stored in the provided path.

#### Run DoD
add dod to python path
```shell
$> export PYTHONPATH="${PYTHONPATH}:/home/cc/aurum-dod-staging"
```
