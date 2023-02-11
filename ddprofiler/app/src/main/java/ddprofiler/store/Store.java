package ddprofiler.store;

import java.util.List;

import ddprofiler.core.Profile;

public interface Store extends ProfilerStore, TextStore {

    void initStore();
    void tearDownStore();

}
