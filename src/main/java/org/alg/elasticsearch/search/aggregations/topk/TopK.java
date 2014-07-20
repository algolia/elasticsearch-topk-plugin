package org.alg.elasticsearch.search.aggregations.topk;

import java.util.Collection;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

/**
 *
 */
public interface TopK extends MultiBucketsAggregation {
    static class Bucket implements MultiBucketsAggregation.Bucket {
        private final String term;
        private final long count;
        
        public Bucket(String term, long count) {
            this.term = term;
            this.count = count;
        }
        
        @Override
        public String getKey() {
            return term;
        }

        @Override
        public Text getKeyAsText() {
            return null; // FIXME
        }

        @Override
        public long getDocCount() {
            return count;
        }

        @Override
        public Aggregations getAggregations() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    Collection<Bucket> getBuckets();

    @SuppressWarnings("unchecked")
    Bucket getBucketByKey(String term);
}
