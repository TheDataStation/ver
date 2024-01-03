# dddprofiler: Tabular data profiler

The ddprofiler is a Java application that generates from input tabular data, a
profiler per columns. A profiler consiste of summary statistics, sketches, and
other metadata useful for downstream tasks.

The ddprofiler can read data from CSV files, databases, and other sources. It
can produce the output profiles as JSON files, or directly index them in
downstream databases, such as elasticsearch.

## Use Cases

TBD

## Setup

The ddprofiler is a Gradle project. Assuming you are in a bash environment,
simply:

```shell
bash build.sh
```

to build the project.

## Indicating input data

The ddprofiler supports tabular data such as tables in databases and CSV files.
You indicate the input data by editing a configuration file, where you indicate
the path where the ddprofiler will find such data.

**--sources** accepts a path to a YAML file in which to configure the access to
the different data sources, e.g., folder with CSV files or JDBC-compatible
RDBMS.

You can find an example template file here which contains documentation to
explain how to use it.

A typical usage of the profiler from the command line will look like:

Example:

```bash
$> bash run.sh --sources <path_to_sources.yml> 
```

You can consult all configuration parameters by appending --help or <?> as a
parameter. 

Note that, although the YAML file accepts any number of data sources, at the
moment we recommend to profile one single source at a time. Note, however, that
you can run ddprofiler as many times as necessary using a YAML with a different
data source. For example, if you want to index a repository of CSV files and a
RDBMS, you will need to run ddprofiler two times, each one configured to read
the data from each source. All data summaries will be created and stored in
elasticsearch. Only make sure to edit the YAML file appropriately each time.

## Enabling and disabling analyzers

There are several analyzer that can be enabled or disabled. To do so, you can
indicate the analyzers you want to enable or disable in the YAML file. There's
a default file for this configuration that you can find in the root folder
called `profile_schema.yml`. To enable or disable an analyzer, simply change
the value of the `enabled` field to `true` or `false` respectively. It's highly
recommended that you use this file as the configuration file, though you can
use any other file as long as it follows the same structure. Here's an example
to use another file:

```bash
$> bash run.sh --sources <path_to_sources.yml> --profile.schema <path_to_profile_schema.yml>
```

### Test datasets

We include a (growing) list of links to datasets you can download to test the
ddprofiler. You can find the list in the test_data/ directory, from the root
folder.

## Outputting profiles as JSON files

If you run the software like this:

```bash
bash run.sh --sources <path_to_yaml_file> --store_type 1
```

each column's profile will be stored in a JSON file inside a folder on the local
directory annotated with the current timestamp.

## Loading profiles to elastic

You can download elastic (we tested with 8.4)
[here](https://www.elastic.co/downloads/enterprise-search).

After downloading and decompressing, [edit the configuration file to disable
security](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-settings.html).

Ensure elastic is running:

```bash
./bin/elastisearch
```

before running the profiler, which you must configure with **--store_type 2** to
load all profiles into elastic.

