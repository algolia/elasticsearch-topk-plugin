package org.alg.elasticsearch.plugin.cardinality;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.List;

import org.alg.elasticsearch.search.aggregations.topk.TopK;
import org.alg.elasticsearch.search.aggregations.topk.TopKBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SimpleTests extends Assert {
    
    private final static ESLogger logger = ESLoggerFactory.getLogger("test");

    protected final String CLUSTER = "test-cluster-" + NetworkUtils.getLocalAddress().getHostName();

    private Node node;
    private Node node2;

    private Client client;

    @BeforeClass
    public void startNode() {
        ImmutableSettings.Builder finalSettings = settingsBuilder()
                .put("cluster.name", CLUSTER)
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("node.local", true)
                .put("gateway.type", "none");
        node = nodeBuilder().settings(finalSettings.put("node.name", "node1").build()).build().start();
        node2 = nodeBuilder().settings(finalSettings.put("node.name", "node2").build()).build().start();

        client = node.client();
    }

    @AfterClass
    public void stopNode() {
        node.close();
        node2.close();
    }

    @Test
    public void assertPluginLoaded() {
        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        logger.info("{}", nodesInfoResponse);
        assertEquals(nodesInfoResponse.getNodes().length, 2);
        assertNotNull(nodesInfoResponse.getNodes()[0].getPlugins().getInfos());
        assertEquals(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), 1);
        assertEquals(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).getName(), "topk-aggregation");
        assertEquals(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).isSite(), false);
    }
    
    @Test
    public void assertTop1OneShard() {
        client.admin().indices().prepareCreate("top1-one-shard").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("top1-one-shard", "type0", "doc0").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("top1-one-shard", "type0", "doc1").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("top1-one-shard", "type0", "doc2").setSource("field0", "bar").setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("top1-one-shard")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("uniq0").field("field0").size(1))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("uniq0");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("foo", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
    }
    
    @Test
    public void assertTop10of3OneShard() {
        client.admin().indices().prepareCreate("top10-one-shard").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("top10-one-shard", "type0", "doc0").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("top10-one-shard", "type0", "doc1").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("top10-one-shard", "type0", "doc2").setSource("field0", "bar").setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("top10-one-shard")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("uniq0").field("field0").size(10))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("uniq0");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(2, buckets.size());
        assertEquals("foo", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
        assertEquals("bar", buckets.get(1).getKey());
        assertEquals(1, buckets.get(1).getDocCount());
    }

    @Test
    public void assertTop10of50OneShard() {
        client.admin().indices().prepareCreate("top10-one-shard2").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("top10-one-shard2", "type0", "doc" + i).setSource("field0", "foo" + i).execute().actionGet();
        }
        client.prepareIndex("top10-one-shard2", "type0", "doc50").setSource("field0", "foo0").setRefresh(true).execute().actionGet(); // foo0 twice
       
        SearchResponse searchResponse = client.prepareSearch("top10-one-shard2")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("uniq0").field("field0").size(10))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("uniq0");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
        for (int i = 1; i < 10; ++i) {
            assertEquals("foo" + i, buckets.get(i).getKey());
            assertEquals(1, buckets.get(i).getDocCount());
        }
    }

    @Test
    public void assertTop10of50TwoShard() {
        client.admin().indices().prepareCreate("top10-one-shard3").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();
        
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("top10-one-shard3", "type0", "doc" + i).setSource("field0", "foo" + i).execute().actionGet();
        }
        for (int i = 50; i < 100; ++i) { // 50 same values
            client.prepareIndex("top10-one-shard3", "type0", "doc" + i).setSource("field0", "foo0").setRefresh(true).execute().actionGet(); // foo0 x 50
        }
       
        SearchResponse searchResponse = client.prepareSearch("top10-one-shard3")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("uniq0").field("field0").size(10))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("uniq0");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(51, buckets.get(0).getDocCount());
    }

}