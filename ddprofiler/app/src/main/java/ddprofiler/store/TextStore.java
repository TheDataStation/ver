package ddprofiler.store;

import java.util.List;

public interface TextStore {

    boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
                      List<String> values);

}
