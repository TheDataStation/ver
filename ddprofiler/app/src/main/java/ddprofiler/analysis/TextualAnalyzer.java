/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.CardinalityAnalyzer;
import ddprofiler.analysis.modules.KMinHash;
import ddprofiler.analysis.modules.LabelAnalyzer;
import ddprofiler.analysis.modules.XSystemAnalyzer;
import ddprofiler.core.config.ProfilerConfig;
import xsystem.layers.XStructure;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private XSystemAnalyzer xa;
    private LabelAnalyzer la;
    private String excludedAnalyzer;

    private TextualAnalyzer(int pseudoRandomSeed, ProfilerConfig pc) {
        excludedAnalyzer = pc.getString(ProfilerConfig.EXCLUDE_ANALYZER);

        analyzers = new ArrayList<>();
        if (!excludedAnalyzer.contains("KMinHash")) {
            mh = new KMinHash(pseudoRandomSeed);
            analyzers.add(mh);
        }

        if (!excludedAnalyzer.contains("Cardinality")) {
            ca = new CardinalityAnalyzer();
            analyzers.add(ca);
        }

        if (!excludedAnalyzer.contains("XSystem")) {
            xa = new XSystemAnalyzer();
            analyzers.add(xa);
        }

        if (!excludedAnalyzer.contains("Label")) {
            la = new LabelAnalyzer(pc);
            analyzers.add(la);
        }
    }

    public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed, ProfilerConfig pc) {
        return new TextualAnalyzer(pseudoRandomSeed, pc);
    }

    @Override
    public boolean feedTextData(List<String> records) {
        Iterator<DataConsumer> dcs = analyzers.iterator();
        while (dcs.hasNext()) {
            TextualDataConsumer dc = (TextualDataConsumer) dcs.next();
            dc.feedTextData(records);
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
        return ca.getCardinality();
    }

    @Override
    public long[] getMH() {
        return mh.getMH();
    }

    @Override
    public XStructure getXstructure() {
        return (xa == null) ? null : xa.getXstructure();
    }

    @Override
    public String getLabel() {
        return (la == null) ? null : la.getLabel();
    }
}
