package ddprofiler.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ddprofiler.analysis.Analysis;
import ddprofiler.analysis.NumericalAnalysis;
import ddprofiler.analysis.TextualAnalysis;
import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.Entities;
import ddprofiler.analysis.modules.Range;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Attribute.AttributeType;

public class ProfileHolder {

    private List<Profile> results;

    /**
     * Used mostly for testing/profiling
     *
     * @return
     */
    public static List<Profile> makeFakeOne() {
        List<Profile> rs = new ArrayList<>();
        Profile wtr = new Profile(
                -1, "none", "none", "none", "none",
                "N", 100, 100, 100, "entities",
                new long[2], 50, 50, 50, 0, 0);
        rs.add(wtr);
        return rs;
    }

    public ProfileHolder(List<Profile> results) {
        this.results = results;
    }

    public ProfileHolder(String dbName, String path, String sourceName, List<Attribute> attributes, Map<String, Analysis> analyzers) {
        List<Profile> rs = new ArrayList<>();
        for (Attribute a : attributes) {
            AttributeType at = a.getColumnType();
            Analysis an = analyzers.get(a.getColumnName());
            long id = Utils.computeAttrId(dbName, sourceName, a.getColumnName());
            if (at.equals(AttributeType.FLOAT)) {
                NumericalAnalysis na = ((NumericalAnalysis) an);
                Cardinality ca = na.getCardinality();
                Range nr = na.getNumericalRange(AttributeType.FLOAT);
                Profile wtr = new Profile(
                        id,
                        dbName,
                        path,
                        sourceName,
                        a.getColumnName(),
                        "N",
<<<<<<< HEAD
                        (int) na.getCardinality().getTotalRecords(),
                        (int) na.getCardinality().getUniqueElements(),
                        (int) na.getCardinality().getNonEmptyValues(),
                        "",
                        null,
                        na.getNumericalRange(AttributeType.FLOAT).getMinF(),
                        na.getNumericalRange(AttributeType.FLOAT).getMaxF(),
                        na.getNumericalRange(AttributeType.FLOAT).getAvg(),
                        na.getNumericalRange(AttributeType.FLOAT).getMedian(),
                        na.getNumericalRange(AttributeType.FLOAT).getIQR());
=======
                        null,
                        (ca != null) ? (int) ca.getTotalRecords() : 0,
                        (ca != null) ? (int) ca.getUniqueElements() : 0,
                        (ca != null) ? (int) ca.getNonEmptyValues() : 0,
                        null,
                        null,
                        null,
                        (nr != null) ? nr.getMinF() : 0,
                        (nr != null) ? nr.getMaxF() : 0,
                        (nr != null) ? nr.getAvg() : 0,
                        (nr != null) ? nr.getMedian() : 0,
                        (nr != null) ? nr.getIQR() : 0);
                        // na.getNumericalRange(AttributeType.FLOAT).getMinF(),
                        // na.getNumericalRange(AttributeType.FLOAT).getMaxF(),
                        // na.getNumericalRange(AttributeType.FLOAT).getAvg(),
                        // na.getNumericalRange(AttributeType.FLOAT).getMedian(),
                        // na.getNumericalRange(AttributeType.FLOAT).getIQR());
>>>>>>> upstream/main
                rs.add(wtr);
            } else if (at.equals(AttributeType.INT)) {
                NumericalAnalysis na = ((NumericalAnalysis) an);
                Cardinality ca = na.getCardinality();
                Range nr = na.getNumericalRange(AttributeType.INT);
                Profile wtr = new Profile(
                        id,
                        dbName,
                        path,
                        sourceName,
                        a.getColumnName(),
                        "N",
<<<<<<< HEAD
                        (int) na.getCardinality().getTotalRecords(),
                        (int) na.getCardinality().getUniqueElements(),
                        (int) na.getCardinality().getNonEmptyValues(),
                        "",
                        null,
                        na.getNumericalRange(AttributeType.INT).getMin(),
                        na.getNumericalRange(AttributeType.INT).getMax(),
                        na.getNumericalRange(AttributeType.INT).getAvg(),
                        na.getNumericalRange(AttributeType.INT).getMedian(),
                        na.getNumericalRange(AttributeType.INT).getIQR());
=======
                        null,
                        (ca != null) ? (int) ca.getTotalRecords() : 0,
                        (ca != null) ? (int) ca.getUniqueElements() : 0,
                        (ca != null) ? (int) ca.getNonEmptyValues() : 0,
                        null,
                        null,
                        null,
                        (nr != null) ? nr.getMin() : 0,
                        (nr != null) ? nr.getMax() : 0,
                        (nr != null) ? nr.getAvg() : 0,
                        (nr != null) ? nr.getMedian() : 0,
                        (nr != null) ? nr.getIQR() : 0);
>>>>>>> upstream/main
                rs.add(wtr);
            } else if (at.equals(AttributeType.STRING)) {
                TextualAnalysis ta = ((TextualAnalysis) an);
                Cardinality ca = ta.getCardinality();
//                Entities e = ta.getEntities();
//                List<String> ents = e.getEntities();
//                StringBuffer sb = new StringBuffer();
//                for (String str : ents) {
//                    sb.append(str);
//                    sb.append(" ");
//                }
//                String entities = sb.toString();

                Profile wtr = new Profile(
                        id,
                        dbName,
                        path,
                        sourceName,
                        a.getColumnName(),
                        "T",
<<<<<<< HEAD
                        (int) ta.getCardinality().getTotalRecords(),
                        (int) ta.getCardinality().getUniqueElements(),
                        (int) ta.getCardinality().getNonEmptyValues(),
                        "entities_removed_on_modernize_ddprofiler",
                        mh,
=======
                        ta.getLabel(),
                        (ca != null) ? (int) ca.getTotalRecords() : 0,
                        (ca != null) ? (int) ca.getUniqueElements() : 0,
                        (ca != null) ? (int) ca.getNonEmptyValues() : 0,
                        "entities_removed_on_modernize_ddprofiler",
                        (ta.getMH() != null) ? ta.getMH() : null,
                        (ta.getXstructure() != null) ? ta.getXstructure().toString() : null,
>>>>>>> upstream/main
                        0,
                        0,
                        0,
                        0,
                        0);
                rs.add(wtr);
            }
        }
        this.results = rs;
    }

    public List<Profile> get() {
        return results;
    }

}