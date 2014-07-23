package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.util.Pair;

/**
 *
 */
public class TopKAggregator extends SingleBucketAggregator {
    
    private ValuesSource.Bytes valuesSource;
    private BytesValues values;
    
    static class Term implements Comparable<Term>, Serializable {
        private static final long serialVersionUID = 9135396685987711497L;

        Term(String term, int bucketOrd) {
            this.term = term;
            this.bucketOrd = bucketOrd;
        }
        Term(String term) {
            this(term, -1);
        }
        String term;
        int bucketOrd;
        
        @Override
        public int compareTo(Term o) {
            return term.compareTo(o.term);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Term) {
                return ((Term) obj).term.equals(term);
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return term.hashCode();
        }
    }
    
    private final Number size;
    private final Number capacity;
    private int currentBucketOrd;
    private final Map<String, Integer> termToBucket;
    private ObjectArray<StreamSummary<Term>> summaries;

    public TopKAggregator(String name, Number size, Number capacity, AggregatorFactories factories, long estimatedBucketsCount, ValuesSource.Bytes valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        this.size = size;
        this.capacity = capacity;
        this.valuesSource = valuesSource;
        this.currentBucketOrd = 0;
        this.termToBucket = new HashMap<>();
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            this.summaries = bigArrays.newObjectArray(initialSize);
        }
    }
    
    @Override
    public boolean shouldCollect() {
        return this.valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        this.values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert this.valuesSource != null : "should collect first";
        
        this.summaries = bigArrays.grow(this.summaries, owningBucketOrdinal + 1);

        StreamSummary<Term> summary = (StreamSummary<Term>) summaries.get(owningBucketOrdinal);
        if (summary == null) {
            summary = new StreamSummary<Term>(capacity.intValue());
            summaries.set(owningBucketOrdinal, summary);
        }

        final int valuesCount = values.setDocument(doc);
        for (int i = 0; i < valuesCount; i++) {
            // store the term
            Term t = new Term(values.nextValue().utf8ToString());
            Pair<Boolean, Term> dropped = summary.offerReturnAll(t, 1);
            
            // assign a bucketOrd
            if (dropped.left) {
                // new item: assign new bucketOrd
                if (this.currentBucketOrd < this.capacity.intValue()) {
                    t.bucketOrd = this.currentBucketOrd++;
                    termToBucket.put(t.term, t.bucketOrd);
                }
            } else {
                // same item: assign same bucketOrd
                t.bucketOrd = this.termToBucket.get(t.term);
            }
            if (dropped.right != null) {
                // recycle bucketOrd (yes, we reuse wrongly aggregated values)
                termToBucket.remove(dropped.right.term);
                if (t.bucketOrd == -1) {
                    t.bucketOrd = dropped.right.bucketOrd;
                    termToBucket.put(t.term, t.bucketOrd);
                }
            }
            
            // collect sub aggregations
            assert t.bucketOrd != -1;
            collectBucket(doc, t.bucketOrd);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        StreamSummary<Term> summary = summaries == null || owningBucketOrdinal >= summaries.size() ? null : (StreamSummary<Term>) summaries.get(owningBucketOrdinal);
        InternalTopK topk = new InternalTopK(name, size, summary);
        for (TopK.Bucket bucket : topk.getBuckets()) {
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
        }
        return topk;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTopK(name, size, null);
    }

    @Override
    public void doClose() {
        if (this.summaries != null) {
            Releasables.close(this.summaries);
        }
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Bytes> {
        private final Number size;
        private final Number capacity;
        
        public Factory(String name, ValuesSourceConfig<ValuesSource.Bytes> valueSourceConfig, Number size, Number capacity) {
            super(name, InternalTopK.TYPE.name(), valueSourceConfig);
            this.size = size;
            this.capacity = capacity;
        }

        @Override
        public Aggregator create(ValuesSource.Bytes valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new TopKAggregator(name, size, capacity, factories, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new TopKAggregator(name, size, capacity, factories, 0, null, aggregationContext, parent);
        }
    }
}
