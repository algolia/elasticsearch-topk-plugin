package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

public class TopKBuilder extends AggregationBuilder<TopKBuilder> {
    
    private String field;
    private Number size;

    public TopKBuilder(String name) {
        super(name, InternalTopK.TYPE.name());
    }

    public TopKBuilder field(String field) {
        this.field = field;
        return this;
    }
    
    public TopKBuilder size(Number size) {
        this.size = size;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (size != null) {
            builder.field("size", size);
        }
        return builder.endObject();
    }

}
