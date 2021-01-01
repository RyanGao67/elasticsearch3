package com.shallowlightning;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class SingletonClient {
    private static RestHighLevelClient client;
    public static RestHighLevelClient getClient(){
        if(client==null){
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")
                    )
            );
            return client;
        }
        return client;
    }
}
