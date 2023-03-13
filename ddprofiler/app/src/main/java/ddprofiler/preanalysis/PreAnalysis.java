/**
 * @author ra-mit
 */
package ddprofiler.preanalysis;

import java.util.List;

import ddprofiler.sources.Source;
import ddprofiler.sources.deprecated.Attribute;

public interface PreAnalysis {

    public void assignSourceTask(Source c);

    public DataQualityReport getQualityReport();

    public List<Attribute> getEstimatedDataTypes();
}
