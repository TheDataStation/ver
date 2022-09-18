package ddprofiler.sources;

// import org.apache.commons.lang.NotImplementedException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ddprofiler.sources.config.CSVSourceConfig;
import ddprofiler.sources.config.HiveSourceConfig;
import ddprofiler.sources.config.PostgresSourceConfig;
import ddprofiler.sources.config.SQLServerSourceConfig;
import ddprofiler.sources.config.SourceConfig;
import ddprofiler.sources.implementations.CSVSource;
import ddprofiler.sources.implementations.HiveSource;
import ddprofiler.sources.implementations.PostgresSource;
import ddprofiler.sources.implementations.SQLServerSource;

public enum SourceType {
    // CSV source
    csv(CSVSourceConfig.class),
    // PostgreSQL relational database source
    postgres(PostgresSourceConfig.class),
    // Microsoft SQL Server database source
    sqlserver(SQLServerSourceConfig.class),
    // Apache Hive data source
    hive(HiveSourceConfig.class);

    Class sc;

    SourceType(Class sc) {
        this.sc = sc;
    }

    Class getConfigClass() {
        return this.sc;
    }

    public static SourceConfig map(SourceType st, JsonNode props, ObjectMapper mapper) throws Exception {
        SourceConfig sc = null;
        for (SourceType s : SourceType.values()) {
            if (st == s) {
                sc = (SourceConfig) mapper.convertValue(props, st.getConfigClass());
            }
        }
        if (sc == null) {
            throw new Exception("SourceType: " + st + " not found. Make sure"
                    + "you register your SourceConfig with its respective class in SourceType.java");
        }
        return sc;
    }

    public static Source instantiateSourceOfType(SourceType st) {
        switch (st) {
            case csv:
                return new CSVSource();
            case postgres:
                return new PostgresSource();
            case sqlserver:
                return new SQLServerSource();
            case hive:
                return new HiveSource();
            default:
                return null;
        }
    }
}
