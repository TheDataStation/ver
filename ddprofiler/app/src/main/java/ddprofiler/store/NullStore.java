package ddprofiler.store;

import java.util.List;

import ddprofiler.core.Profile;

public class NullStore implements Store {

    @Override
    public void initStore() {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
                             List<String> values) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storeProfile(Profile wtr) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void tearDownStore() {
        // TODO Auto-generated method stub
    }
}
