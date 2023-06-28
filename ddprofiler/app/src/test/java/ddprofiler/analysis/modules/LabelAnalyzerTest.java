package ddprofiler.analysis.modules;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import ddprofiler.core.config.ProfilerConfig;

/**
 * Unit test for LabelAnalyzer.
 * 
 * Note that the test cases are not exhaustive.
 * The assertion results are based on the of XStructure reference file and the similarity threshold.
 * Keep in mind it's not going to be 100% accurate at determining label.
 */
@ExtendWith(MockitoExtension.class)
public class LabelAnalyzerTest {

    private final double SIMILARITY_THRESHOLD = 0.2;
    private final boolean XSYSTEM_ENABLED = true;
    private final String REFERENCE_FILE_PATH = ".\\src\\main\\resources\\reference.json";

    @Mock
    private ProfilerConfig pc;
    private LabelAnalyzer autolabel;
    
    @Test
    public void testDateAndTime() {
        final String EXPECTED = "Date & Time";
        when(pc.getBoolean("xsystem")).thenReturn(XSYSTEM_ENABLED);
        when(pc.getDouble("xsystem.threshold")).thenReturn(SIMILARITY_THRESHOLD);
        when(pc.getString("xsystem.reference")).thenReturn(REFERENCE_FILE_PATH);

        LabelAnalyzer.nullifyXStructureReference();
        autolabel = new LabelAnalyzer(pc);
        ArrayList<String> test = new ArrayList<>();

        test.add("12/10/2002");
        test.add("02/11/2002");
        test.add("22/11/2012");
        test.add("18/01/2002");
        test.add("07/12/2002");
        test.add("11/12/2012");

        autolabel.feedTextData(test);
        assertEquals(EXPECTED, autolabel.getLabel());
        test.clear();

        test.add("12-10-2002");
        test.add("02-11-2002");
        test.add("22-11-2012");
        test.add("18-01-2002");
        test.add("07-12-2002");
        test.add("11-12-2012");

        autolabel.feedTextData(test);
        assertNotEquals(EXPECTED, autolabel.getLabel());
        test.clear();

        test.add("05/30/2023 00:00:00 AM");
        test.add("06/30/2023 09:01:01 PM");
        test.add("07/30/2023 10:40:32 AM");
        test.add("08/30/2023 01:29:57 PM");
        test.add("09/02/2023 01:04:10 AM");
        
        autolabel.feedTextData(test);
        assertEquals(EXPECTED, autolabel.getLabel());
        test.clear();
        
        test.add("05-30-2023 00:00:00 AM");
        test.add("06-30-2023 09:01:01 PM");
        test.add("07-30-2023 10:40:32 AM");
        test.add("08-30-2023 01:29:57 PM");
        test.add("09-02-2023 01:04:10 AM");

        autolabel.feedTextData(test);
        assertNotEquals(EXPECTED, autolabel.getLabel());
        test.clear();

        test.add("00:00:00");
        test.add("19:01:01");
        test.add("10:40:32");
        test.add("21:29:57");
        test.add("01:04:10");

        autolabel.feedTextData(test);
        assertNotEquals(EXPECTED, autolabel.getLabel());
        test.clear();

    }

    @Test
    public void testLocation() {
        final String EXPECTED = "Location";
        when(pc.getBoolean("xsystem")).thenReturn(XSYSTEM_ENABLED);
        when(pc.getDouble("xsystem.threshold")).thenReturn(SIMILARITY_THRESHOLD);
        when(pc.getString("xsystem.reference")).thenReturn(REFERENCE_FILE_PATH);

        LabelAnalyzer.nullifyXStructureReference();
        autolabel = new LabelAnalyzer(pc);
        ArrayList<String> test = new ArrayList<>();

        test.add("(12.31341441, -31.31414141)");
        test.add("(12.31341441, 31.31414185)");
        test.add("(-12.31341441, 31.31414141)");
        test.add("(112.31341414, -31.31414114)");
        test.add("(-110.31341414, 31.312087614)");
        test.add("(-12.31341414, -31.314674414)");

        autolabel.feedTextData(test);
        assertEquals(EXPECTED, autolabel.getLabel());
        test.clear();

        test.add("Point(12.313414414, -31.314141414)");
        test.add("Point(12.313414414, 31.314141685)");
        test.add("Point(-12.313414414, 19.314141414)");
        test.add("Point(112.313414414, -31.314141414)");
        test.add("Point(-110.313414414, 31.3120876614)");
        test.add("Point(-12.313414414, -31.3146741414)");

        autolabel.feedTextData(test);
        assertNotEquals(EXPECTED, autolabel.getLabel());
        test.clear();

    }

    @Test
    public void testZipCode() {
        final String EXPECTED = "Zip";
        when(pc.getBoolean("xsystem")).thenReturn(XSYSTEM_ENABLED);
        when(pc.getDouble("xsystem.threshold")).thenReturn(SIMILARITY_THRESHOLD);
        when(pc.getString("xsystem.reference")).thenReturn(REFERENCE_FILE_PATH);

        LabelAnalyzer.nullifyXStructureReference();
        autolabel = new LabelAnalyzer(pc);
        ArrayList<String> test = new ArrayList<>();

        test.add("00716");
        test.add("12345");
        test.add("10928");
        test.add("11000");
        test.add("12098");
        test.add("09871");

        autolabel.feedTextData(test);
        assertEquals(EXPECTED, autolabel.getLabel());
        test.clear();

    }

    @Test
    public void testNoReference() {
        final String EXPECTED = null;
        when(pc.getBoolean("xsystem")).thenReturn(XSYSTEM_ENABLED);
        when(pc.getDouble("xsystem.threshold")).thenReturn(SIMILARITY_THRESHOLD);
        when(pc.getString("xsystem.reference")).thenReturn(REFERENCE_FILE_PATH);

        LabelAnalyzer.nullifyXStructureReference();
        autolabel = new LabelAnalyzer(pc);
        ArrayList<String> test = new ArrayList<>();

        test.add("460 Tulip Street");
        test.add("62 Orchard Street");
        test.add("3 Main Street");
        test.add("1100 Main Street");
        test.add("1234 Avocado Street");
        test.add("55 Apple Street");

        autolabel.feedTextData(test);
        assertEquals(EXPECTED, autolabel.getLabel());
        test.clear();

    }

}
