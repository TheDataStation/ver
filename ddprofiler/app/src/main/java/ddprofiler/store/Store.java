package ddprofiler.store;

import java.util.List;

import ddprofiler.core.Profile;

public interface Store {

    void initStore();

    boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
                      List<String> values);

    boolean storeProfile(Profile wtr);

    void tearDownStore();
}
