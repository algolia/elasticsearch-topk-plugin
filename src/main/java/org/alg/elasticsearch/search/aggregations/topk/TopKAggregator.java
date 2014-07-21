package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import com.clearspring.analytics.stream.StreamSummary;

/**
 *
 */
public class TopKAggregator extends SingleBucketAggregator {
    
    private ValuesSource.Bytes valuesSource;
    private BytesValues values;
    
    private final Number size;
    private final Number capacity;
    private ObjectArray<StreamSummary<String>> summaries;

    public TopKAggregator(String name, Number size, Number capacity, AggregatorFactories factories, long estimatedBucketsCount, ValuesSource.Bytes valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        this.size = size;
        this.capacity = capacity;
        this.valuesSource = valuesSource;
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

        StreamSummary<String> summary = (StreamSummary<String>) summaries.get(owningBucketOrdinal);
        if (summary == null) {
            summary = new StreamSummary<String>(capacity.intValue());
            summaries.set(owningBucketOrdinal, summary);
        }

        final int valuesCount = values.setDocument(doc);
        for (int i = 0; i < valuesCount; i++) {
            summary.offer(values.nextValue().utf8ToString());
        }
        
        collectBucketNoCounts(doc, owningBucketOrdinal); // FIXME: we don't know the bucketOrd here
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        StreamSummary<String> summary = summaries == null || owningBucketOrdinal >= summaries.size() ? null : (StreamSummary<String>) summaries.get(owningBucketOrdinal);
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
