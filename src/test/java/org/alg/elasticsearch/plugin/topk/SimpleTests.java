package org.alg.elasticsearch.plugin.topk;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.alg.elasticsearch.search.aggregations.topk.TopK;
import org.alg.elasticsearch.search.aggregations.topk.TopKBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
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
                .put("gateway.type", "none")
        		.put("cluster.routing.allocation.disk.threshold_enabled", false);
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
        client.admin().indices().prepareCreate("topk-0").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("topk-0", "type0", "doc0").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("topk-0", "type0", "doc1").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("topk-0", "type0", "doc2").setSource("field0", "bar").setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("topk-0")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(1))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("foo", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
    }
    
	public String getMapping() {
		try {
			XContentBuilder mapping = jsonBuilder();
			mapping.startObject()
				.field("logs")
					.startObject()
						.field("dynamic_templates")
							.startArray()
								.startObject()
									.field("string_fields")
										.startObject()
											.field("mapping")
												.startObject()
													.field("type", "multi_field")
													.field("fields")
														.startObject()
															.field("raw")
																.startObject()
																	.field("index", "not_analyzed")
																	.field("ignore_above", 256)
																	.field("type", "string")
																.endObject()
															.field("{name}")
																.startObject()
																	.field("index", "not_analyzed")
																	.field("omit_norms", true)
																	.field("type", "string")
																.endObject()
														.endObject()
												.endObject()
											.field("match", "*")
											.field("match_mapping_type", "string")
										.endObject()
								.endObject()
							.endArray()
						.field("_all")
							.startObject()
								.field("enabled", false)
							.endObject()
						.field("_timestamp")
							.startObject()
								.field("enabled", true)
								.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
								.field("format", "YYYY-MM-dd HH:mm:ss")
							.endObject()
						.field("_ttl")
							.startObject()
								.field("enabled", true)
								.field("default", 86400000)
							.endObject()
						.field("_source")
							.startObject()
								.field("enabled", false)
							.endObject()
						.field("properties")
							.startObject()
								.field("action")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("appID")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("appIDIndex")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("hour")
									.startObject()
										.field("type", "integer")
									.endObject()
								.field("index")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("ip")
									.startObject()
										.field("type", "ip")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("country")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("nbChar")
									.startObject()
										.field("type", "long")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("nbExactMatch")
									.startObject()
										.field("type", "long")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("nbHits")
									.startObject()
										.field("type", "long")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("nbWord")
									.startObject()
										.field("type", "long")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("query")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
											.field("format", "doc_values")
												.field("loading", "eager_global_ordinals")
											.endObject()
									.endObject()
								.field("referer")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("refined_query")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
											.field("format", "doc_values")
												.field("loading", "eager_global_ordinals")
											.endObject()
									.endObject()
								.field("refinements")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
												.field("loading", "eager_global_ordinals")
											.endObject()
									.endObject()
								.field("refinements.raw")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
												.field("loading", "eager_global_ordinals")
											.endObject()
									.endObject()
								.field("tags")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("timeMS")
									.startObject()
										.field("type", "long")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
								.field("timeout")
									.startObject()
										.field("type", "boolean")
									.endObject()
								.field("userAgent")
									.startObject()
										.field("type", "string")
										.field("index", "not_analyzed")
										.field("doc_values", true)
										.field("fielddata")
											.startObject()
												.field("format", "doc_values")
											.endObject()
									.endObject()
							.endObject()
					.endObject()
			.endObject();
			return mapping.string();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
    
    @Test
    public void docValuesTest() {
        client.admin().indices().create(new CreateIndexRequest("topk-42").mapping("logs", getMapping()).settings(ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.store.throttle.type", "none"))).actionGet().isAcknowledged();
        final int N = 100000;
        for (int i = 0; i <= N; ++i) {
            client.prepareIndex("topk-42", "logs", "doc" + i).setSource("refinements", "foo" + i).setRefresh(i == N).execute().actionGet();
        }
        System.out.println("========================= INDEXING DONE");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {}
        System.out.println("========================= SEARCH");
        SearchResponse searchResponse = client.prepareSearch("topk-42")
                .setQuery(matchAllQuery())
                .addAggregation(new TermsBuilder("terms").field("refinements").size(1))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("term");
        assertNotNull(topk);
        System.gc();
        while (true);
    }
    
    @Test
    public void assertTop1NumericalOneShard() {
        client.admin().indices().prepareCreate("topk-0n").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("topk-0n", "type0", "doc0").setSource("field0", 42).execute().actionGet();
        client.prepareIndex("topk-0n", "type0", "doc1").setSource("field0", 42).execute().actionGet();
        client.prepareIndex("topk-0n", "type0", "doc2").setSource("field0", 51).setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("topk-0n")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(1))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("42", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
    }
    
    @Test
    public void assertTop1BooleanOneShard() {
        client.admin().indices().prepareCreate("topk-0b").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("topk-0b", "type0", "doc0").setSource("field0", true).execute().actionGet();
        client.prepareIndex("topk-0b", "type0", "doc1").setSource("field0", true).execute().actionGet();
        client.prepareIndex("topk-0b", "type0", "doc2").setSource("field0", false).setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("topk-0b")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(1))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(1, buckets.size());
        assertEquals("T", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
    }
    
    @Test
    public void assertTop10of3OneShard() {
        client.admin().indices().prepareCreate("topk-1").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        client.prepareIndex("topk-1", "type0", "doc0").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("topk-1", "type0", "doc1").setSource("field0", "foo").execute().actionGet();
        client.prepareIndex("topk-1", "type0", "doc2").setSource("field0", "bar").setRefresh(true).execute().actionGet();
       
        SearchResponse searchResponse = client.prepareSearch("topk-1")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("topk");
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
        client.admin().indices().prepareCreate("topk-2").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("topk-2", "type0", "doc" + i).setSource("field0", "foo" + i).execute().actionGet();
        }
        client.prepareIndex("topk-2", "type0", "doc50").setSource("field0", "foo0").setRefresh(true).execute().actionGet(); // foo0 twice
       
        SearchResponse searchResponse = client.prepareSearch("topk-2")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10))
                .execute().actionGet();
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(2, buckets.get(0).getDocCount());
        for (int i = 1; i < 10; ++i) {
            assertEquals(1, buckets.get(i).getDocCount());
        }
    }

    @Test
    public void assertTop10of50TwoShard() {
        client.admin().indices().prepareCreate("topk-3").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();
        
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("topk-3", "type0", "doc" + i).setSource("field0", "foo" + i).setRefresh(true).execute().actionGet();
        }
        for (int i = 50; i < 100; ++i) { // 50 same values
            client.prepareIndex("topk-3", "type0", "doc" + i).setSource("field0", "foo0").setRefresh(true).execute().actionGet();
        }
       
        SearchResponse searchResponse = client.prepareSearch("topk-3")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10))
                .execute().actionGet();
        assertEquals(100, searchResponse.getHits().getTotalHits());
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(51, buckets.get(0).getDocCount());
    }

    @Test
    public void assertTop10of50OneShardNestedAggregations() {
        client.admin().indices().prepareCreate("topk-4").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        
        double sum = 0;
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("topk-4", "type0", "doc" + i).setSource("{ \"field0\": \"foo" + i + "\", \"field1\":" + i +" }").setRefresh(true).execute().actionGet();
        }
        
        // foo0 x 50
        for (int i = 50; i < 100; ++i) {
            client.prepareIndex("topk-4", "type0", "doc" + i).setSource("{ \"field0\": \"foo0\", \"field1\":" + i +" }").setRefresh(true).execute().actionGet();
            sum += i;
        }
       
        SearchResponse searchResponse = client.prepareSearch("topk-4")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10)
                        .subAggregation(new AvgBuilder("avg").field("field1"))
                        .subAggregation(new MaxBuilder("max").field("field1"))
                )
                .execute().actionGet();
        assertEquals(100, searchResponse.getHits().getTotalHits());
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(51, buckets.get(0).getDocCount());
        assertEquals(2, buckets.get(0).getAggregations().asList().size());
        for (Aggregation agg : buckets.get(0).getAggregations()) {
            switch (agg.getName()) {
            case "avg":
                assertEquals(sum / 51, ((Avg) agg).getValue(), 0.01);
                break;
            case "max":
                assertEquals(99.0, ((Max) agg).getValue(), 0.001);
                break;
            default:
                assertTrue(false);
            } 
        }
    }
    
    @Test
    public void assertTop10of50TwoShardNestedAggregations() {
        client.admin().indices().prepareCreate("topk-5").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();
        
        double sum = 0;
        for (int i = 0; i < 50; ++i) { // 50 values
            client.prepareIndex("topk-5", "type0", "doc" + i).setSource("{ \"field0\": \"foo" + i + "\", \"field1\":" + i +" }").setRefresh(true).execute().actionGet();
        }
        
        // foo0 x 50
        for (int i = 50; i < 100; ++i) {
            client.prepareIndex("topk-5", "type0", "doc" + i).setSource("{ \"field0\": \"foo0\", \"field1\":" + i +" }").setRefresh(true).execute().actionGet();
            sum += i;
        }
       
        SearchResponse searchResponse = client.prepareSearch("topk-5")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10)
                        .subAggregation(new AvgBuilder("avg").field("field1"))
                        .subAggregation(new MaxBuilder("max").field("field1"))
                )
                .execute().actionGet();
        assertEquals(100, searchResponse.getHits().getTotalHits());
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foo0", buckets.get(0).getKey());
        assertEquals(51, buckets.get(0).getDocCount());
        assertEquals(2, buckets.get(0).getAggregations().asList().size());
        for (Aggregation agg : buckets.get(0).getAggregations()) {
            switch (agg.getName()) {
            case "avg":
                assertEquals(sum / 51, ((Avg) agg).getValue(), 0.01);
                break;
            case "max":
                assertEquals(99.0, ((Max) agg).getValue(), 0.001);
                break;
            default:
                assertTrue(false);
            } 
        }
    }
    
    @Test
    public void assertTop10of50TwoShardNestedAggregationsStress() {
        client.admin().indices().prepareCreate("topk-6").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 4)).execute().actionGet();
        
        final int N = 30000;
        double sum = 0;
        int n = 0;
        int max = Integer.MIN_VALUE;
        Random r = new Random();
        for (int i = 0; i < N; ++i) {
            int v = r.nextInt();
            if (i % 7 == 0) {
                sum += v;
                ++n;
                if (v > max) {
                    max = v;
                }
            }
            client.prepareIndex("topk-6", "type0", "doc" + i).setSource("{ \"field0\": \"foo" + (i % 7 == 0 ? "bar" : String.valueOf(i)) + "\", \"field1\":" + v +" }").setRefresh(i == N - 1).execute().actionGet();
        }
        try { Thread.sleep(2000); } catch (InterruptedException e) {} // FIXME: wait until all docs are searchable
        
        SearchResponse searchResponse = client.prepareSearch("topk-6")
                .setQuery(matchAllQuery())
                .addAggregation(new TopKBuilder("topk").field("field0").size(10)
                        .subAggregation(new AvgBuilder("avg").field("field1"))
                        .subAggregation(new MaxBuilder("max").field("field1"))
                )
                .execute().actionGet();
        assertEquals(N, searchResponse.getHits().getTotalHits());
        TopK topk = searchResponse.getAggregations().get("topk");
        assertNotNull(topk);
        List<TopK.Bucket> buckets = new ArrayList<>(topk.getBuckets());
        assertEquals(10, buckets.size());
        assertEquals("foobar", buckets.get(0).getKey());
        assertEquals(n, buckets.get(0).getDocCount());
        assertEquals(2, buckets.get(0).getAggregations().asList().size());
        for (Aggregation agg : buckets.get(0).getAggregations()) {
            switch (agg.getName()) {
            case "avg":
                assertEquals(sum / n, ((Avg) agg).getValue(), 0.01);
                break;
            case "max":
                assertEquals((double) max, ((Max) agg).getValue(), 0.001);
                break;
            default:
                assertTrue(false);
            } 
        }
    }
}
