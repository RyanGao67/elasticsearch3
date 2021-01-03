package com.shallowlightning;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.xcontent.XContentType;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
public class App {
    private static RestHighLevelClient client = SingletonClient.getClient();
    public static void main(String[] args) throws URISyntaxException, IOException {
//        deleteIndex();
//        createTemplate();
//        indexIndex();
        checkAllocate();
        client.close();
    }

    // for testing you may need this
    // curl -XPUT localhost:9200/_cluster/settings -H 'Content-Type:application/json' -d \
    // '{"persistent" : {"cluster.routing.allocation.enable":"primaries"}}'
    // reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-reroute.html
    // reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html
    public static void checkAllocate() throws IOException {
        System.out.println("nodes: ");
        ArrayNode nodes = catNodes();   nodes.forEach(node->System.out.println(node));
        System.out.println("shards: ");
        ArrayNode shards = catShards();     shards.forEach(shard->System.out.println(shard));
        shards.forEach(shard -> {
            if(shard.get("state").textValue().equals("UNASSIGNED") && shard.get("prirep").textValue().equals("r")){
                try {allocateReplica(shard.get("index").textValue(), shard.get("shard").intValue(), "172.19.0.4" );}
                catch (IOException e) {    e.printStackTrace();                }
            }else if(shard.get("state").textValue().equals("UNASSIGNED") && shard.get("prirep").textValue().equals("r")){
                try                   {    allocatePrimary();                }
                catch (IOException e) {    e.printStackTrace();              }
            }
        });
    }

    public static boolean createTemplate() throws URISyntaxException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("my-template");
        // Step 1: Get the template json path
        // Step 2: Read the template json
        URL defaultURL = Thread.currentThread().getContextClassLoader().getResource("templates"+"/risk_scores.json");  System.out.println(defaultURL);
        //  file:/home/ryangao67/Downloads/untitled1/build/resources/main/templates/risk_scores.json
        URI defaultURI = defaultURL.toURI();                                                                                 System.out.println(defaultURI);
        //  file:/home/ryangao67/Downloads/untitled1/build/resources/main/templates/risk_scores.json
        Path templatePath = Paths.get(defaultURI);                                                                           System.out.println(templatePath);
        //  /home/ryangao67/Downloads/untitled1/build/resources/main/templates/risk_scores.json
        String template = null;
        try{
            StringBuilder contents = new StringBuilder();
            Stream<String> ss = Files.lines(templatePath);
            for(String line:ss.collect(Collectors.toList())){
                contents.append(line).append(System.getProperty("line.separator"));
            }
            ss.close();
            template =  contents.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Step 2: send the request
        request.source(template, XContentType.JSON);
        // In case multiple templates match an index, the orders of matching templates determine the sequence
        // that settings, mappings, and alias of each matching template is applied. Templates with lower orders
        // are applied first, and higher orders override them.
        request.order(20);
        // A template can optionally specify a version number which can be used to simplify template management
        // by external systems.
        request.version(4);
        request.masterNodeTimeout("1m");
        try{
            AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(request, RequestOptions.DEFAULT);
            if(putTemplateResponse.isAcknowledged()){
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean indexIndex() throws IOException, URISyntaxException {
        Map<String, String> metadata = new HashMap();
        metadata.put("routing", "entityHash");
        metadata.put("id", "id");
        index("sample", "risk_scores", metadata);
        return true;
    }

    public static void index(String document, String indexName, Map<String, String > metadata) throws IOException, URISyntaxException {
        ObjectMapper mapper = new ObjectMapper();
        ////////////////////////////////////////////////////////////////////////////////////////////// reusable code, *remember this*
        URL records = Thread.currentThread().getContextClassLoader().getResource("sample.json");
        URI recordsURI = records.toURI();
        Path recordsPath = Paths.get(recordsURI);
        String doc = null;
        try{
            StringBuilder docSB = new StringBuilder();
            Stream<String> ss = Files.lines(recordsPath);
            for(String s:ss.collect(Collectors.toList())){
                docSB.append(s);
            }
            ss.close();
            doc = docSB.toString();
        }catch (IOException e){
            System.out.println(e.toString());
        }
/////////////////////////////////////////Another approach
//        String doc = IOUtils.toString(
//                App.class.getResourceAsStream(String.format("/%s.json", document)),
//                StandardCharsets.UTF_8
//        );
/////////////////////////////////////////
        System.out.println(doc);
        JsonNode jsonDocuments = mapper.readTree(doc);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        jsonDocuments.fields().forEachRemaining(jsonNode -> index(jsonNode.getValue(), indexName, metadata));
    }

    public static void index(JsonNode jsonDocuments, String index, Map<String, String> metadata){
        int id = 0;
        for(JsonNode node:jsonDocuments){
            System.out.println(node.toString());
            id++;
            IndexRequest indexRequest = new IndexRequest(index, "_doc");
            // source the indexrequest
            indexRequest.source(node.toString(), XContentType.JSON);
            // get the metadata for this doc: id
            if(metadata.containsKey("id") && node.has(metadata.get("id"))){
                String docId = node.get(metadata.get("id")).asText();
                indexRequest.id(docId);
            }else{
                indexRequest.id(String.valueOf(id));
            }
            // get the metadata for this doc: routing
            if(metadata.containsKey("routing") && node.has(metadata.get("routing"))){
                indexRequest.routing(node.get(metadata.get("routing")).asText());
            }else if(index.startsWith("anomalies")){
                String routingKey = "id";
                if(node.has("rollupLevel") && node.get("rollupLevel").has("parent")){
                    routingKey = "aggId";
                }
                indexRequest.routing(node.get(routingKey).asText());
            }
            // if document has parent, add it to routing
            if(metadata.containsKey("parent") && node.has(metadata.get("parent"))){
                String parentId = node.get(metadata.get("parent")).asText();
                indexRequest.routing(parentId);
            }
            try{
                client.index(indexRequest,RequestOptions.DEFAULT);
            }catch (IOException e){
                throw new RuntimeException("Failed to index document with index: "+index);
            }
        }
    }

    public static ArrayNode catShards() throws IOException {
        String endpoint = String.format("/_cat/shards?format=json");
        Request request = new Request("GET", endpoint);
        Response response = client.getLowLevelClient().performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        ArrayNode shards = (ArrayNode) new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readTree(result);
        return shards;
    }

    public static ArrayNode catNodes() throws IOException {
        String endpoint = String.format("/_cat/nodes?format=json");
        Request request = new Request("GET", endpoint);
        Response response = client.getLowLevelClient().performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        ArrayNode nodes = (ArrayNode) new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readTree(result);
        return nodes;
    }

    //curl --silent -XPOST "localhost:9200/_cluster/reroute" -H "Content-Type:application/json"
    // -d '{"commands":[{"allocate_replica":{"index":"risk_scores", "shard":0, "node":"172.19.0.4"}}]}'
    public static void allocateReplica(String index, int shard, String target) throws IOException {
        try {
            Request request = new Request(
                    "POST",
                    "/_cluster/reroute");
            request.setJsonEntity(
                String.format(
                    "{\"commands\":[{\"allocate_replica\":{\"index\":\"%s\", \"shard\":%s, \"node\":\"%s\"}}]}",
                    index,
                    shard,
                    target
                )
            );
            Response response = client.getLowLevelClient().performRequest(request);
        }catch (Exception e){
            System.out.println(e.toString());
        }
    }

    // curl --silent -XPOST "localhost:9200/_cluster/reroute" -H "Content-Type:application/json" -d
    // '{"commands":[{"allocate_stale_primary":{"index":"risk_scores", "shard":0, "node":"es03", "accept_data_loss":true}}]}'
    public static void allocatePrimary() throws IOException {
        try {
            Request request = new Request(
                    "POST",
                    "/_cluster/reroute");
            request.setJsonEntity(
                String.format(
                    "{\"commands\":[{\"allocate_stale_primary\":{\"index\":\"%s\", \"shard\":%s, \"node\":\"%s\", \"accept_data_loss\":true}}]}",
                    "risk_scores",
                    0,
                    "172.19.0.4"
                )
            );
            Response response = client.getLowLevelClient().performRequest(request);
        }catch (Exception e){
            System.out.println(e.toString());
        }
    }

    public static boolean deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("risk_scores");
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return deleteIndexResponse.isAcknowledged();
    }
}