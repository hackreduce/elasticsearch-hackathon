package com.traackr.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * @author gstathis
 *         Created on: 9/26/13
 */
public class ESClient {
  
  static final InetSocketTransportAddress[] hosts = {
      new InetSocketTransportAddress("cluster-7-slave-00.sl.hackreduce.net", 9300),
      new InetSocketTransportAddress("cluster-7-slave-09.sl.hackreduce.net", 9300),
      new InetSocketTransportAddress("cluster-7-slave-19.sl.hackreduce.net", 9300) };
  
  public static void main(String args[]) throws Exception {
    Settings settings = ImmutableSettings.settingsBuilder()
                                         .put("cluster.name", "cluster-7-slave-00")
                                         .put("client.transport.ignore_cluster_name", true)
                                         .build();
    Client client = new TransportClient(settings).addTransportAddresses(hosts);
    
    SearchResponse result = client.prepareSearch("wikipedia")
                                  .setTypes("article")
                                  .setQuery(QueryBuilders.matchQuery("title", "search"))
                                  .addField("title")
                                  .setSize(20)
                                  .execute()
                                  .actionGet();
    System.out.println(String.format("Found %s hits in %s millis:",
                                     result.getHits().getTotalHits(),
                                     result.getTookInMillis()));
    for (SearchHit hit : result.getHits()) {
      System.out.println(hit.getFields().get("title").values());
    }
    client.close();
  }
}
