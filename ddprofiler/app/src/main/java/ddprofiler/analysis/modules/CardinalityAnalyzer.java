/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis.modules;

import java.util.List;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import ddprofiler.analysis.FloatDataConsumer;
import ddprofiler.analysis.IntegerDataConsumer;
import ddprofiler.analysis.TextualDataConsumer;

public class CardinalityAnalyzer
        implements IntegerDataConsumer, FloatDataConsumer, TextualDataConsumer {

    private long totalRecords;
    private long nonEmpty;
    private ICardinality ic;

    public CardinalityAnalyzer() {
        // ic = new HyperLogLogPlus(4, 16);
        ic = new HyperLogLogPlus(18, 25);
    }

    public Cardinality getCardinality() {
        long uniqueElements = ic.cardinality();
        Cardinality c = new Cardinality(totalRecords, uniqueElements, nonEmpty);
        return c;
    }

    @Override
    public boolean feedTextData(List<String> records) {
        for (String r : records) {
            if (r != null && !r.isEmpty() && !r.trim().isEmpty()) {
                nonEmpty++;
            }
            totalRecords++;
            ic.offer(r);
        }

        return true;
    }

    @Override
    public boolean feedFloatData(List<Float> records) {

        for (float r : records) {
            if (r != 0) {
                nonEmpty++;
            }
            totalRecords++;
            ic.offer(r);
        }

        return true;
    }

    @Override
    public boolean feedIntegerData(List<Long> records) {

        for (long r : records) {
            if (r != 0) {
                nonEmpty++;
            }
            totalRecords++;
            ic.offer(r);
        }

        return true;
    }
}
