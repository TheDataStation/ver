/**
 *
 */
package ddprofiler.analysis;

import ddprofiler.analysis.modules.Range;
import ddprofiler.sources.deprecated.Attribute.AttributeType;

/**
 * @author Raul - raulcf@csail.mit.edu
 * Sibo (edit)
 */
public interface NumericalAnalysis
        extends Analysis, IntegerDataConsumer, FloatDataConsumer {

    public Range getNumericalRange(AttributeType type);

    // add an interface to return the quantile
    public long getQuantile(double p);
}
