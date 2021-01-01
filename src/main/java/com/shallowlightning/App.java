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
//        catShards();
//        catNodes();
        allocateReplica();
        allocatePrimary();
        client.close();
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
//        String doc = IOUtils.toString(
//                App.class.getResourceAsStream(String.format("/%s.json", document)),
//                StandardCharsets.UTF_8
//        );
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

    public static void catShards() throws IOException {
        String endpoint = String.format("/_cat/shards?format=json");
        Request request = new Request("GET", endpoint);
        Response response = client.getLowLevelClient().performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        ArrayNode shards = (ArrayNode) new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readTree(result);
        System.out.println(shards);
//        {"index":"risk_scores","shard":"0","prirep":"p","state":"STARTED","docs":"55","store":"24.7kb","ip":"172.19.0.3","node":"es01"}
        shards.forEach(shard -> {
            System.out.println("index: "+shard.get("index"));
            System.out.println("shard: "+shard.get("shard"));
            System.out.println("prirep: "+shard.get("prirep"));
            System.out.println("state: "+shard.get("state"));
            System.out.println("docs: "+shard.get("docs"));
            System.out.println("store: "+shard.get("store"));
            System.out.println("ip: "+shard.get("ip"));
            System.out.println("node: "+shard.get("node"));System.out.println();
        });
    }

    public static void catNodes() throws IOException {
        String endpoint = String.format("/_cat/nodes?format=json");
        Request request = new Request("GET", endpoint);
        Response response = client.getLowLevelClient().performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        ArrayNode nodes = (ArrayNode) new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readTree(result);
        System.out.println(nodes);
//        {"ip":"172.19.0.4","heap.percent":"67","ram.percent":"98","cpu":"19","load_1m":"1.47","load_5m":"2.09","load_15m":"4.04","node.role":"cdhilmrstw","master":"-","name":"es03"},
        nodes.forEach(shard -> {
            System.out.println("ip: "+shard.get("ip"));
            System.out.println("heap.percent: "+shard.get("heap.percent"));
            System.out.println("ram.percent: "+shard.get("ram.percent"));
            System.out.println("cpu: "+shard.get("cpu"));
            System.out.println("load_1m: "+shard.get("load_1m"));
            System.out.println("load_5m: "+shard.get("load_5m"));
            System.out.println("load_15m: "+shard.get("load_15m"));
            System.out.println("node.role: "+shard.get("node.role"));
            System.out.println("master: "+shard.get("master"));
            System.out.println("name: "+shard.get("name"));
            System.out.println();
        });
    }

    //curl --silent -XPOST "localhost:9200/_cluster/reroute" -H "Content-Type:application/json"
    // -d '{"commands":[{"allocate_replica":{"index":"risk_scores", "shard":0, "node":"172.19.0.4"}}]}'
    public static void allocateReplica() throws IOException {
        try {
            Request request = new Request(
                    "POST",
                    "/_cluster/reroute");
            request.setJsonEntity(
                String.format(
                    "{\"commands\":[{\"allocate_replica\":{\"index\":\"%s\", \"shard\":%s, \"node\":\"%s\"}}]}",
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