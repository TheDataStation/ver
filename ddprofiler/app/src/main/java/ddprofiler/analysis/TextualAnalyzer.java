/**
 * @author Raul - raulcf@csail.mit.edu
 */
package ddprofiler.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ddprofiler.analysis.modules.*;
import xsystem.layers.XStructure;

public class TextualAnalyzer implements TextualAnalysis {

    private List<DataConsumer> analyzers;
    private CardinalityAnalyzer ca;
    private KMinHash mh;
    private XSystemAnalyzer xa;
    private EntityAnalyzer ea;

    private TextualAnalyzer(int pseudoRandomSeed) {
        analyzers = new ArrayList<>();
        mh = new KMinHash(pseudoRandomSeed);
        ca = new CardinalityAnalyzer();
        xa = new XSystemAnalyzer();
//        this.ea = ea;
        analyzers.add(ca);
        analyzers.add(mh);
        analyzers.add(xa);
//        analyzers.add(ea);
    }

    public static TextualAnalyzer makeAnalyzer(int pseudoRandomSeed) {
//        ea2.clear();
        return new TextualAnalyzer(pseudoRandomSeed);
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

//    @Override
//    public Entities getEntities() {
//        return null;
//    }

    @Override
    public long[] getMH() {
        return mh.getMH();
    }

    @Override
    public XStructure getXstructure() {
        return xa.getXstructure();
    }

}
