package ddprofiler.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ddprofiler.core.Profile;
import ddprofiler.core.config.ProfilerConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

public class JSONFilesStore implements Store {
    
    private String outputPath;

    public JSONFilesStore(ProfilerConfig pc) throws Exception {
        String outputFolder = pc.getString(ProfilerConfig.STORE_TYPE_JSON_OUTPUT_FOLDER);
        File f = new File(outputFolder);
        if (! f.canWrite()) {
            throw new Exception("Can't write to target output folder");
        }
        this.outputPath = f.getPath();
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

        try (PrintWriter out = new PrintWriter(new FileWriter(this.outputPath + "/" + fileName))) {
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
