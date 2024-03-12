package ddprofiler.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import com.opencsv.exceptions.CsvValidationException;
import ddprofiler.core.config.ProfilerConfig;
import ddprofiler.sources.Source;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import ddprofiler.preanalysis.PreAnalyzer;
import ddprofiler.preanalysis.Values;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Attribute.AttributeType;
import ddprofiler.sources.implementations.CSVSource;
import ddprofiler.sources.implementations.PostgresSource;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PreAnalyzerTest {

    @Mock
    private ProfilerConfig profilerConfig;
    @Mock
    private Source source;
    private PreAnalyzer preAnalyzer;
    private int numRecords = 10;

    /**
     * Setup PreAnalyzer before each test
     * (specifically for new tests, not legacy ones).
     */
    @Before
    public void setUp() throws CsvValidationException, SQLException, IOException {
        // Avoid Exceptions by adding a sample column to the source.
        when(source.getAttributes()).thenReturn(List.of(new Attribute("sample")));

        // Initialize and set source of PreAnalyzer
        preAnalyzer = new PreAnalyzer(profilerConfig);
        preAnalyzer.assignSourceTask(source);
    }

    /**
     * Test readRows method for spatial data, ensuring correct
     * semantic types and granularity.
     */
    @Test
    public void testReadRowsSpatialData() throws CsvValidationException, SQLException, IOException {
        // Prepare sample spatial columns & read the rows
        Map<Attribute, List<String>> spatialData = getSpatialData();
        when(source.readRows(1)).thenReturn(spatialData);
        preAnalyzer.readRows(1);

        // Ensuring correct semantic type & granularity
        for (Entry<Attribute, List<String>> dataEntry : spatialData.entrySet()) {
            Attribute attribute = dataEntry.getKey();
            assertEquals(Attribute.AttributeSemanticType.SPATIAL, attribute.getColumnSemanticType());

            String columnName = attribute.getColumnName();
            if (columnName.startsWith("geo_coordinate")) {
                assertEquals("geoCoordinate", attribute.getColumnSemanticTypeDetails().get("granularity"));
            } else if (columnName.startsWith("street")) {
                assertEquals("street", attribute.getColumnSemanticTypeDetails().get("granularity"));
            } else if (columnName.startsWith("zip_code")) {
                assertEquals("zipCode", attribute.getColumnSemanticTypeDetails().get("granularity"));
            } else {
                assertEquals("state", attribute.getColumnSemanticTypeDetails().get("granularity"));
            }
        }
    }

    private Map<Attribute, List<String>> getSpatialData() {
        Map<Attribute, List<String>> spatialData = new HashMap<>();
        spatialData.put(
                new Attribute("geo_coordinate1"), List.of("POINT(41.919365236 -87.769726946)")
        );
        spatialData.put(
                new Attribute("geo_coordinate2"), List.of("(41.90643°, -87.703717°)")
        );
        spatialData.put(
                new Attribute("street1"), List.of("23RD ST")
        );
        spatialData.put(
                new Attribute("street2"), List.of("1958 North Milwaukee Avenue")
        );
        spatialData.put(
                new Attribute("zip_code"), List.of("60007")
        );
        spatialData.put(
                new Attribute("state"), List.of("IL")
        );
        return spatialData;
    }

    public void typeChecking(PreAnalyzer pa) {

        pa.readRows(numRecords);
        List<Attribute> attrs = pa.getEstimatedDataTypes();
        for (Attribute a : attrs) {
            System.out.println(a);
        }

        Map<Attribute, Values> data = pa.readRows(numRecords);
        for (Entry<Attribute, Values> a : data.entrySet()) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("NAME: " + a.getKey().getColumnName());
            System.out.println("TYPE: " + a.getKey().getColumnType());
            if (a.getKey().getColumnType().equals(AttributeType.FLOAT)) {
                List<Float> objs = a.getValue().getFloats();
                for (float f : objs) {
                    System.out.println(f);
                }
            }
            if (a.getKey().getColumnType().equals(AttributeType.STRING)) {
                List<String> objs = a.getValue().getStrings();
                for (String f : objs) {
                    System.out.println(f);
                }
            }
        }
    }

    public void workloadTest(List<String> test_strings, PreAnalyzer pa) {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < test_strings.size(); i++) {
            pa.isNumericalException(test_strings.get(i));
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Exception method took: " + (endTime - startTime) + " milliseconds");
        System.out.println("----------------------------------------------------------------\n\n");

        System.out.println("Using Reg Exp based solution with workloads that are all numerical values");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < test_strings.size(); i++) {
            pa.isNumerical(test_strings.get(i));
        }
        endTime = System.currentTimeMillis();
        System.out.println("Reg Exp based method took: " + (endTime - startTime) + " milliseconds");
    }

    @Test
    public void testRegExpPerformance() {
        PreAnalyzer pa = new PreAnalyzer(null);
        final int NUM_TEST_STRINGS = 1000000;
        final double DOUBLE_RANGLE_MIN = 1.0;
        final double DOUBLE_RANGLE_MAX = 10000000.0;
        List<String> testStrings = new Vector<String>();
        double start = DOUBLE_RANGLE_MIN;
        double end = DOUBLE_RANGLE_MAX;
        Random randomSeeds = new Random();

        for (int i = 0; i < NUM_TEST_STRINGS; i++) {
            double randomGen = randomSeeds.nextDouble();
            double result = start + (randomGen * (end - start));
            testStrings.add(result + "");
        }

        // testing workloads that are numberical values, in this case the
        // try-catch
        // approach will not never incur an exception
        System.out.println("Test with workloads that are all numerical values");
        workloadTest(testStrings, pa);
        System.out.println("----------------------------------------------------------------\n\n");

        for (int i = 0; i < NUM_TEST_STRINGS / 2; i++) {
            testStrings.set(i, "A");
        }

        System.out.println("Test with workloads that half of them are numerical values");
        workloadTest(testStrings, pa);
        System.out.println("----------------------------------------------------------------\n\n");

        for (int i = NUM_TEST_STRINGS / 2; i < NUM_TEST_STRINGS; i++) {
            testStrings.set(i, "A");
        }
        System.out.println("Test with workloads that all them are numerical values");
        workloadTest(testStrings, pa);
        System.out.println("----------------------------------------------------------------\n\n");
    }

    @Test
    public void testPreAnalyzerForTypesCSVFile() throws IOException {

        // FIXME: create config on the fly
        CSVSource fc = new CSVSource();

        PreAnalyzer pa = new PreAnalyzer(null);
        pa.assignSourceTask(fc);
        System.out.println("------------begin type checking with FileConnector");
        typeChecking(pa);
        System.out.println("------------finish type checking with FileConnector");
    }

    @Test
    public void testPreAnalyzerForTypesDB() throws IOException {

        // Old_DBConnector dbc = new Old_DBConnector("", DBType.MYSQL, connIP,
        // port, sourceName, tableName, username,
        // password);

        // FIXME: create config on the fly
        PostgresSource dbc = new PostgresSource();

        PreAnalyzer pa = new PreAnalyzer(null);
        pa.assignSourceTask(dbc);
        System.out.println("------------begin type checking with DBConnector");
        typeChecking(pa);
        System.out.println("------------finish type checking with DBConnector");
    }
}
