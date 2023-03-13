package ddprofiler.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ddprofiler.core.Profile;
import ddprofiler.core.config.ProfilerConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class JSONFilesStore implements Store {

//     private static final SimpleDateFormat tsPattern = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss-SSSXXX");
    private String outputPath;

    public JSONFilesStore(ProfilerConfig pc) throws Exception {
//         Timestamp ts = new Timestamp(System.currentTimeMillis());
//         String timestamp = tsPattern.format(ts);
        String outputFolder = pc.getString(ProfilerConfig.STORE_TYPE_JSON_OUTPUT_FOLDER);
//         String outputFolderName = outputFolder + "_" + timestamp;
        String outputFolderName = outputFolder;
        Files.createDirectories(Paths.get(outputFolderName));
        this.outputPath = outputFolderName;
    }

    @Override
    public void initStore() {
        // done via the constructor
    }

    @Override
    /*
    Ignore
     */
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName, List<String> values) {
        // we don't index data when using the json file store type
        return false;
    }

    @Override
    /*
    Create a JSON file per Profile
     */
    public boolean storeProfile(Profile wtr) {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(wtr);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        String fileName = wtr.dbName() + "." + wtr.sourceName() + "." + wtr.columnName() + "." + wtr.id() + ".json";

        String filePath = this.outputPath + "/" + fileName;
        System.out.println("filepath: " + filePath);
        try (PrintWriter out = new PrintWriter(new FileWriter(filePath))) {
            assert json != null;
            out.write(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /*
    Tear down any open/pending resources
     */
    @Override
    public void tearDownStore() {

    }
}
