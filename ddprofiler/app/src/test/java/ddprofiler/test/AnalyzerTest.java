package ddprofiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import ddprofiler.analysis.AnalyzerFactory;
import ddprofiler.analysis.NumericalAnalysis;
import ddprofiler.analysis.TextualAnalysis;
import ddprofiler.analysis.modules.Cardinality;
import ddprofiler.analysis.modules.Entities;
import ddprofiler.analysis.modules.Range;
import ddprofiler.preanalysis.PreAnalyzer;
import ddprofiler.preanalysis.Values;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Attribute.AttributeType;
import ddprofiler.sources.implementations.CSVSource;

public class AnalyzerTest {

    private String path = "C:\\csv\\";
    private String filename = "dataset1.csv";
    // private String path = "/Users/ra-mit/Desktop/mitdwhdata/";
    // private String filename = "short_cis_course_catalog.csv";
    private String separator = ",";
    private int numRecords = 100;
    private String db = "mysql";
    private String connIP = "localhost";
    private String port = "3306";
    private String sourceName = "/test";
    private String tableName = "nellsimple";
    private String username = "root";
    private String password = "Qatar";

    @Test
    public void test() throws IOException {

        // Old_FileConnector fc = new Old_FileConnector("", path, filename,
        // separator);

        // FIXME: configure source on-the-fly
        CSVSource fc = new CSVSource();

        // DBConnector dbc = new DBConnector(db, connIP, port, sourceName,
        // tableName, username, password);
        PreAnalyzer pa = new PreAnalyzer(null);
        pa.assignSourceTask(fc);
        // pa.composeConnector(dbc);

        Map<Attribute, Values> data = pa.readRows(numRecords);
        Map<Attribute, TextualAnalysis> taMapping = new HashMap<Attribute, TextualAnalysis>();
        for (Entry<Attribute, Values> a : data.entrySet()) {
            System.out.println("CName: " + a.getKey().getColumnName());
            System.out.println("CType: " + a.getKey().getColumnType());
            AttributeType at = a.getKey().getColumnType();
            if (at.equals(AttributeType.FLOAT)) {
                NumericalAnalysis na = AnalyzerFactory.makeNumericalAnalyzer();
                List<Float> floats = new ArrayList<>();
                for (Float s : a.getValue().getFloats()) {
                    floats.add(s);
                }

                na.feedFloatData(floats);
                Cardinality c = na.getCardinality();
                Range r = na.getNumericalRange(AttributeType.FLOAT);
                System.out.println("Cardinality:");
                System.out.println(c);
                System.out.println("Range:");
                System.out.println(r);
                System.out.println("median: " + na.getQuantile(0.5));
            }
            if (at.equals(AttributeType.STRING)) {
                TextualAnalysis ta = AnalyzerFactory.makeTextualAnalyzer(1);
                taMapping.put(a.getKey(), ta);
                List<String> strs = new ArrayList<>();
                for (String s : a.getValue().getStrings()) {
                    strs.add(s);
                }

                ta.feedTextData(strs);
                Cardinality c = ta.getCardinality();
                Entities e = ta.getEntities();
                System.out.println("Cardinality:");
                System.out.println(c);
                System.out.println("Entities:");
                System.out.println(e);
            }
            break;
        }

        for (int i = 0; i < 20; i++) {
            data = pa.readRows(numRecords);
            if (data == null)
                break;
            for (Entry<Attribute, Values> a : data.entrySet()) {
                System.out.println("CName: " + a.getKey().getColumnName());
                System.out.println("CType: " + a.getKey().getColumnType());
                AttributeType at = a.getKey().getColumnType();
                if (at.equals(AttributeType.FLOAT)) {
                    NumericalAnalysis na = AnalyzerFactory.makeNumericalAnalyzer();
                    List<Float> floats = new ArrayList<>();
                    for (Float s : a.getValue().getFloats()) {
                        floats.add(s);
                    }

                    na.feedFloatData(floats);
                    Cardinality c = na.getCardinality();
                    Range r = na.getNumericalRange(AttributeType.FLOAT);
                    System.out.println("Cardinality:");
                    System.out.println(c);
                    System.out.println("Range:");
                    System.out.println(r);
                    System.out.println("median: " + na.getQuantile(0.5));
                }
                if (at.equals(AttributeType.STRING)) {
                    TextualAnalysis ta = taMapping.get(a.getKey());
                    List<String> strs = new ArrayList<>();
                    for (String s : a.getValue().getStrings()) {
                        strs.add(s);
                    }

                    ta.feedTextData(strs);
                    Cardinality c = ta.getCardinality();
                    Entities e = ta.getEntities();
                    System.out.println("Cardinality:");
                    System.out.println(c);
                    System.out.println("Entities:");
                    System.out.println(e);
                }
                break;
            }
        }
    }
}
