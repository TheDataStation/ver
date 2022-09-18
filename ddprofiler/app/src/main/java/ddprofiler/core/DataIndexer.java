package ddprofiler.core;

import java.util.Map;

import ddprofiler.preanalysis.Values;
import ddprofiler.sources.deprecated.Attribute;

public interface DataIndexer {

    public boolean indexData(String dbName, String path, Map<Attribute, Values> data);

    public boolean flushAndClose();
}
