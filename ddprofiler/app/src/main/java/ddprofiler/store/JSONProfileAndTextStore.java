package ddprofiler.store;

import com.opencsv.CSVWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ddprofiler.core.Profile;
import ddprofiler.core.Worker;
import ddprofiler.core.config.ProfilerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class JSONProfileAndTextStore implements Store {

    final private Logger LOG = LoggerFactory.getLogger(JSONProfileAndTextStore.class.getName());
//     private static final SimpleDateFormat tsPattern = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss-SSSXXX");
    private final String profileOutputPath;
    private final String textOutputPath;
    private final int MAX_CSV_SIZE = 200 * 1024 * 1024; // 200 MB
    // attributes related to text files
    private int currentFileIndex = -1;
    private int currentFileSizeEstimate = 0;
    private CSVWriter csvWriter;

    public JSONProfileAndTextStore(ProfilerConfig pc) throws Exception {
//         Timestamp ts = new Timestamp(System.currentTimeMillis());
//         String timestamp = tsPattern.format(ts);
        String outputFolder = pc.getString(ProfilerConfig.STORE_TYPE_JSON_OUTPUT_FOLDER);
        // folder for storing json files representing profiles
//         String outputJSONFolderName = outputFolder + "_" + timestamp + "/json/";
        String outputJSONFolderName = outputFolder + "/json/";
        Files.createDirectories(Paths.get(outputJSONFolderName));
        this.profileOutputPath = outputJSONFolderName;
        // folder for storing text files
//         String outputTextFolderName = outputFolder + "_" + timestamp + "/text/";
        String outputTextFolderName = outputFolder + "/text/";
        Files.createDirectories(Paths.get(outputTextFolderName));
        this.textOutputPath = outputTextFolderName;
    }

    @Override
    public void initStore() {
        // done via the constructor
    }

    @Override
    public void tearDownStore() {
        // release resources related to text files
        try {
            this.csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

        String filePath = this.profileOutputPath + "/" + fileName;
        System.out.println("filepath: " + filePath);
        try (PrintWriter out = new PrintWriter(new FileWriter(filePath))) {
            assert json != null;
            out.write(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private void rollFile() {
        File newFile = new File(textOutputPath + (currentFileIndex + 1) + ".csv");
        CSVWriter csvWriter;
        try {
            FileWriter fw = new FileWriter(newFile);
            csvWriter = new CSVWriter(fw);
            this.csvWriter = csvWriter;
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.currentFileIndex += 1; // if successful, we update the index
        LOG.info("Rolled new text file: " + newFile.getAbsolutePath());
    }

    @Override
    public synchronized boolean indexData(long id, String dbName, String path, String sourceName, String columnName, List<String> values) {
        // if csvWriter is null this is the first call, so we roll out a file
        if (csvWriter == null) {
            this.rollFile();
            String[] header = {"id", "dbName", "path", "sourceName", "columnName", "data"};
            this.csvWriter.writeNext(header);
        }

        // write data
        String data = String.join(" ", values);
        String[] line = {Long.toString(id), dbName, path, sourceName, columnName, data};
        this.csvWriter.writeNext(line);

        // check if we crossed the file lenght limit
        int recordSize = (dbName.length() + path.length() + sourceName.length() +
                columnName.length() + data.length()) * Character.SIZE;
        this.currentFileSizeEstimate += recordSize;
        // if we cross the threshold, we roll a new file
        if (this.currentFileSizeEstimate > this.MAX_CSV_SIZE) {
            try {
                this.csvWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.rollFile();
            this.currentFileSizeEstimate = 0;
        }
        return true;
    }
}
