package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation.CommonFields;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

/**
 *
 */
public interface TopK extends MultiBucketsAggregation {
    static class Bucket implements MultiBucketsAggregation.Bucket {
        private final String term;
        private final long count;
        private final InternalAggregations aggregations;
        
        public Bucket(String term, long count, InternalAggregations aggregations) {
            this.term = term;
            this.count = count;
            this.aggregations = aggregations;
        }
        
        @Override
        public String getKey() {
            return term;
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(term));
        }

        @Override
        public long getDocCount() {
            return count;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }
        
        Bucket reduce(List<Bucket> buckets, BigArrays bigArrays) {
            long docCount = 0;
            List<InternalAggregations> aggregationsList = Lists.newArrayListWithCapacity(buckets.size());
            for (Bucket bucket : buckets) {
                docCount += bucket.getDocCount();
                aggregationsList.add((InternalAggregations) bucket.getAggregations());
            }
            final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, bigArrays);
            return new Bucket(term, docCount, aggs);
        }

        void toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, term);
            builder.field(CommonFields.DOC_COUNT, count);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }
    }
    
    Collection<Bucket> getBuckets();

    @SuppressWarnings("unchecked")
    Bucket getBucketByKey(String term);
}
