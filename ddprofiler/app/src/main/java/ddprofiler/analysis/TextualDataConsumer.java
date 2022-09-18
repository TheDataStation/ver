/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.List;

public interface TextualDataConsumer extends DataConsumer {

    public boolean feedTextData(List<String> records);
}
