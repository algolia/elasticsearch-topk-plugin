package org.alg.elasticsearch.plugin.topk;

import org.alg.elasticsearch.search.aggregations.topk.InternalTopK;
import org.alg.elasticsearch.search.aggregations.topk.TopKParser;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;

public class TopKPlugin extends AbstractPlugin {

    public String name() {
        return "topk-aggregation";
    }

    public String description() {
        return "Top-K Aggregation for Elasticsearch";
    }
    
    public void onModule(AggregationModule module) {
        module.addAggregatorParser(TopKParser.class);
        InternalTopK.registerStreams();
    }

}
