/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.DataType;

public interface Analysis {

    public DataProfile getProfile();

    public Cardinality getCardinality();
}
