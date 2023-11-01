/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.core.config;

import java.util.List;
import java.util.Map;

import ddprofiler.core.config.ConfigDef.Importance;
import ddprofiler.core.config.ConfigDef.Type;

public class ProfilerConfig extends Config {

    private static final ConfigDef config;

<<<<<<< HEAD
=======
    public static final String PROFILE_SCHEMA_FILE = "profile.schema";
    private static final String PROFILE_SCHEMA_FILE_DOC = "Path to the YAML file with the " + "profile schema";

>>>>>>> aff10a7cee35a7ef0ed44b20946e857e0c226c57
    public static final String EXPERIMENTAL = "experimental";
    private static final String EXPERIMENTAL_DOC = "To activate experimental features";

    public static final String SOURCE_CONFIG_FILE = "sources";
    private static final String SOURCE_CONFIG_FILE_DOC = "Path to the YAML file with the " + "source configuration";

    public static final String EXECUTION_MODE = "execution.mode";
    private static final String EXECUTION_MODE_DOC = "(online - 0) for server mode "
            + "and (offline files - 1) for one-shot read files from directory "
            + "and (offline db - 2) for one-shot read tables from db, (benchmark 3) " + "for benchmarking purposes";

    public static final String WEB_SERVER_PORT = "web.server.port";
    private static final String WEB_SERVER_PORT_DOC = "The port where web " + "server listens";

    public static final String NUM_POOL_THREADS = "num.pool.threads";
    private static final String NUM_POOL_THREADS_DOC = "Number of threads " + "in the worker pool";

    public static final String NUM_RECORD_READ = "num.record.read";
    private static final String NUM_RECORD_READ_DOC = "Number of records to "
            + "read per interaction with the data sources";

    public static final String STORE_TYPE = "store.type";
    private static final String STORE_TYPE_DOC = "Configures store type: "
            + "NULL(0), ELASTIC_HTTP(1), ELASTIC_NATIVE(2)";

    public static final String STORE_HTTP_PORT = "store.http.port";
    private static final String STORE_HTTP_PORT_DOC = "Server HTTP port for " + "stores that support it";

    public static final String STORE_SERVER = "store.server";
    private static final String STORE_SERVER_DOC = "Server name or IP where " + "the store lives";

    public static final String STORE_PORT = "store.port";
    private static final String STORE_PORT_DOC = "Port where the store listens";

//    public static final String ELASTIC_TEXT_MAPPING_FILE = "elastic.textmapping.file";
//    private static final String ELASTIC_TEXT_MAPPING_FILE_DOC = "Path to JSON file with text mapping";
//
//    public static final String ELASTIC_PROFILE_MAPPING_FILE = "elastic.textmapping.file";
//    private static final String ELASTIC_PROFILE_MAPPING_FILE_DOC = "Path to JSON file with text mapping";

    public static final String STORE_TYPE_JSON_OUTPUT_FOLDER = "store.json.output.folder";
    private static final String STORE_TYPE_JSON_OUTPUT_FOLDER_DOC = "Folder to store all JSON file output";

    public static final String ERROR_LOG_FILE_NAME = "error.logfile.name";
    private static final String ERROR_LOG_FILE_NAME_DOC = "Name of log file "
            + "that records the errors while profiling data";

    public static final String REPORT_METRICS_CONSOLE = "console.metrics";
    private static final String REPORT_METRICS_CONSOLE_DOC = "Output metrics to console";

    static {
        config = new ConfigDef()
                .define(SOURCE_CONFIG_FILE, Type.STRING, "", Importance.HIGH, SOURCE_CONFIG_FILE_DOC)
                .define(EXECUTION_MODE, Type.INT, 0, Importance.HIGH, EXECUTION_MODE_DOC)
                .define(WEB_SERVER_PORT, Type.INT, 8080, Importance.MEDIUM, WEB_SERVER_PORT_DOC)
                .define(NUM_POOL_THREADS, Type.INT, 8, Importance.LOW, NUM_POOL_THREADS_DOC)
                .define(NUM_RECORD_READ, Type.INT, 1000, Importance.MEDIUM, NUM_RECORD_READ_DOC)
                .define(STORE_TYPE, Type.INT, 3, Importance.MEDIUM, STORE_TYPE_DOC) // 1 -> JSON
                .define(STORE_SERVER, Type.STRING, "127.0.0.1", Importance.HIGH, STORE_SERVER_DOC)
                .define(STORE_HTTP_PORT, Type.INT, 9200, Importance.HIGH, STORE_HTTP_PORT_DOC)
                .define(STORE_TYPE_JSON_OUTPUT_FOLDER, Type.STRING, "./output_profiles_json",
                    Importance.MEDIUM, STORE_TYPE_JSON_OUTPUT_FOLDER_DOC)
                .define(STORE_PORT, Type.INT, 9200, Importance.HIGH, STORE_PORT_DOC)
//                .define(ELASTIC_TEXT_MAPPING_FILE, Type.STRING, "", Importance.HIGH, ELASTIC_TEXT_MAPPING_FILE_DOC)
//                .define(ELASTIC_PROFILE_MAPPING_FILE, Type.STRING, "", Importance.HIGH, ELASTIC_PROFILE_MAPPING_FILE_DOC)
                .define(ERROR_LOG_FILE_NAME, Type.STRING, "error_profiler.log", Importance.MEDIUM,
                    ERROR_LOG_FILE_NAME_DOC)
                .define(REPORT_METRICS_CONSOLE, Type.INT, -1, Importance.HIGH, REPORT_METRICS_CONSOLE_DOC)
<<<<<<< HEAD
                .define(EXPERIMENTAL, Type.BOOLEAN, false, Importance.LOW, EXPERIMENTAL_DOC);
=======
                .define(EXPERIMENTAL, Type.BOOLEAN, false, Importance.LOW, EXPERIMENTAL_DOC)
                .define(PROFILE_SCHEMA_FILE, Type.STRING, "./../profile_schema.yml", Importance.HIGH, PROFILE_SCHEMA_FILE_DOC)
                .define(XSYSTEM_REFERENCE_FILE, Type.STRING, "./app/src/main/resources/reference.json", 
                    Importance.MEDIUM, XSYSTEM_REFERENCE_FILE_DOC)
                .define(XSYSTEM_SIMILARITY_THRESHOLD, Type.DOUBLE, 0.5, 
                    Importance.MEDIUM, XSYSTEM_SIMILARITY_THRESHOLD_DOC);
>>>>>>> aff10a7cee35a7ef0ed44b20946e857e0c226c57
    }

    public ProfilerConfig(Map<? extends Object, ? extends Object> originals) {
        super(config, originals);
    }

    public static ConfigKey getConfigKey(String name) {
        return config.getConfigKey(name);
    }

    public static List<ConfigKey> getAllConfigKey() {
        return config.getAllConfigKey();
    }

    public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }
}
