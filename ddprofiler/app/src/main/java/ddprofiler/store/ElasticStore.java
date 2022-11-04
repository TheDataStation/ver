package ddprofiler.store;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import ddprofiler.core.Profile;
import ddprofiler.core.config.ProfilerConfig;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class ElasticStore implements Store {

    final private Logger LOG = LoggerFactory.getLogger(ElasticStore.class.getName());

    private final String serverUrl;
    private final String storeServer;
    private final int storePort;

    private ElasticsearchClient client;

    public ElasticStore(ProfilerConfig pc) {
        String storeServer = pc.getString(ProfilerConfig.STORE_SERVER);
        int storePort = pc.getInt(ProfilerConfig.STORE_PORT);
        int storeHttpPort = pc.getInt(ProfilerConfig.STORE_HTTP_PORT);

        this.storeServer = storeServer;
        this.storePort = storePort;
        // note https instead of http as in elastic version > 8 security is enabled by default
        this.serverUrl = "https://" + storeServer + ":" + storeHttpPort;
    }

    private void createTextIndexAndMapping() {
        LOG.info("Creating text index...");
        try {
            boolean exists = client.indices().exists(c -> c.index("text")).value();
            if(!exists) {
                client.indices().create(c -> c.index("text"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        PutMappingRequest.Builder putTextMappingRequestBuilder = new PutMappingRequest.Builder();
        putTextMappingRequestBuilder.index("text");

        String textMapping =
                "{\n" +
                        "    \"properties\": {\n" +
                        "        \"id\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"store\": \"true\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"dbName\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"path\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"sourceName\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"columnName\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"text\": {\n" +
                        "            \"type\": \"text\",\n" +
                        "            \"store\": \"false\",\n" +
                        "            \"index\": \"true\",\n" +
                        //                        "            \"analyzer\": \"english\",\n" + // avoiding analyzers for now
                        "            \"term_vector\": \"yes\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "}";
        putTextMappingRequestBuilder.withJson(new StringReader(textMapping));
        PutMappingRequest putTextMappingRequest = putTextMappingRequestBuilder.build();
        PutMappingResponse putTextMappingResponse = null;
        try {
            putTextMappingResponse = client.indices().putMapping(putTextMappingRequest);
            System.out.println(putTextMappingResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info("Creating text index... OK");
    }

    private void createProfileIndexAndMapping() {
        LOG.info("Creating profile index...");
        try {
            boolean exists = client.indices().exists(c -> c.index("profile")).value();
            if(!exists) {
                client.indices().create(c -> c.index("profile"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        PutMappingRequest.Builder putProfileMappingRequestBuilder = new PutMappingRequest.Builder();
        putProfileMappingRequestBuilder.index("profile");

        String profileMapping =
                "{\n" +
                        "    \"properties\": {\n" +
                        "        \"id\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"dbName\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"path\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"sourceNameNA\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"sourceName\": {\n" +
                        "            \"type\": \"text\",\n" +
                        "            \"index\": \"true\"\n" +
//                        "            \"analyzer\": \"aurum_analyzer\"\n" +
                        "        },\n" +
                        "        \"columnNameNA\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"columnName\": {\n" +
                        "            \"type\": \"text\",\n" +
                        "            \"index\": \"true\"\n" +
//                        "            \"analyzer\": \"aurum_analyzer\"\n" +
                        "        },\n" +
                        "        \"dataType\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"totalValues\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"uniqueValues\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"nonEmptyValues\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"entities\": {\n" +
                        "            \"type\": \"keyword\",\n" +
                        "            \"index\": \"true\"\n" +
                        "        },\n" +
                        "        \"minhash\": {\n" +
                        "            \"type\": \"long\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"minValue\": {\n" +
                        "            \"type\": \"double\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"maxValue\": {\n" +
                        "            \"type\": \"double\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"avgValue\": {\n" +
                        "            \"type\": \"double\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"median\": {\n" +
                        "            \"type\": \"double\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        },\n" +
                        "        \"iqr\": {\n" +
                        "            \"type\": \"double\",\n" +
                        "            \"index\": \"false\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "}";
        putProfileMappingRequestBuilder.withJson(new StringReader(profileMapping));
        PutMappingRequest putProfileMappingRequest = putProfileMappingRequestBuilder.build();
        PutMappingResponse putProfileMappingResponse = null;
        try {
            putProfileMappingResponse = client.indices().putMapping(putProfileMappingRequest);
            System.out.println(putProfileMappingResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info("Creating text index...OK");
    }

    @Override
    public void initStore() {
        // Create elastic client
        // Create the low-level client
        RestClient restClient = RestClient.builder(new HttpHost(this.storeServer, this.storePort)).build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        this.client = new ElasticsearchClient(transport);

        // Create text index and mapping
        this.createTextIndexAndMapping();

        // Create profile index and mapping
        this.createProfileIndexAndMapping();

    }

    @Override
    public boolean indexData(long id, String dbName, String path, String sourceName, String columnName, List<String> values) {

        class Wrapper {
            public long id;
            public String dbName;
            public String path;
            public String sourceName;
            public String columnName;
            public List<String> text;
            public Wrapper(long id, String dbName, String path, String sourceName, String columnName, List<String> values){
                this.id = id;
                this.dbName = dbName;
                this.path = path;
                this.sourceName = sourceName;
                this.columnName = columnName;
                this.text = values;
            }
        }

        Wrapper document = new Wrapper(id, dbName, path, sourceName, columnName, values);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(i -> i.index("text").id(String.valueOf(id)).document(document));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // if there was no exception earlier this worked
        return true;
    }

    @Override
    public boolean storeProfile(Profile wtr) {

        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(i -> i.index("profile").id(String.valueOf(wtr.id())).document(wtr));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // if there was no exception earlier this worked
        return true;

    }

    @Override
    public void tearDownStore() {
        client.shutdown();
    }
}
