/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.config.AnalyzerConfig;
import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.CardinalityAnalyzer;
import ddprofiler.analysis.modules.Range;
import ddprofiler.analysis.modules.RangeAnalyzer;
import ddprofiler.sources.deprecated.Attribute.AttributeType;

public class NumericalAnalyzer implements NumericalAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private RangeAnalyzer ra;

    private NumericalAnalyzer() {
        analyzers = new ArrayList<>();

        if (AnalyzerConfig.getCardinality().getEnabled()) {
            ca = new CardinalityAnalyzer();
            analyzers.add(ca);
        }

        if (AnalyzerConfig.getRange().getEnabled()) {
            ra = new RangeAnalyzer();
            analyzers.add(ra);
        }
    }

    public static NumericalAnalyzer makeAnalyzer() {
        return new NumericalAnalyzer();
    }

    @Override
    public boolean feedIntegerData(List<Long> records) {

        Iterator<DataConsumer> dcs = analyzers.iterator();
        while (dcs.hasNext()) {
            IntegerDataConsumer dc = (IntegerDataConsumer) dcs.next();
            dc.feedIntegerData(records);
        }

        return false;
    }

    @Override
    public boolean feedFloatData(List<Float> records) {

        Iterator<DataConsumer> dcs = analyzers.iterator();
        while (dcs.hasNext()) {
            FloatDataConsumer dc = (FloatDataConsumer) dcs.next();
            dc.feedFloatData(records);
        }

        return false;
    }

    @Override
    public DataProfile getProfile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Cardinality getCardinality() {
        return (ca != null) ? ca.getCardinality() : null;
    }

    @Override
    public Range getNumericalRange(AttributeType type) {
        if (type.equals(AttributeType.FLOAT)) {
            return (ra != null) ? ra.getFloatRange() : null;
        } else if (type.equals(AttributeType.INT)) {
            return (ra != null) ? ra.getIntegerRange() : null;
        }
        return null;
    }

    @Override
    public long getQuantile(double p) {
        return (ra != null) ? ra.getQuantile(p) : null;
    }
}
