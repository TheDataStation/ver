/**
 * @author ra-mit
 */
package ddprofiler.preanalysis;

import java.util.Map;

import ddprofiler.sources.deprecated.Attribute;

public interface IO {
    public Map<Attribute, Values> readRows(int num) throws Exception;
}
