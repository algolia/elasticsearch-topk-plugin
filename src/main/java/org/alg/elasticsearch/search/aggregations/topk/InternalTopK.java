package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

/**
*
*/
public class InternalTopK extends InternalAggregation implements TopK {

    public final static Type TYPE = new Type("topk");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalTopK readResult(StreamInput in) throws IOException {
            InternalTopK result = new InternalTopK();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private StreamSummary<String> summary;
    private Number size;
    private List<TopK.Bucket> buckets;
    private HashMap<String, TopK.Bucket> bucketsMap;

    InternalTopK() { }  // for serialization

    InternalTopK(String name, Number size, StreamSummary<String> summary) {
        super(name);
        this.size = size;
        this.summary = summary;
        this.buckets = new ArrayList<>();
        this.bucketsMap = null;
        if (this.summary != null) {
            List<Counter<String>> counters = this.summary.topK(this.size.intValue());
            int bucketOrd = 0;
            for (Counter<String> c : counters) {
                this.buckets.add(new TopK.Bucket(c.getItem(), c.getCount(), bucketOrd++, null));
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TopK.Bucket getBucketByKey(String term) {
        if (bucketsMap == null) {
            for (TopK.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketsMap.get(term);
    }
    
    @Override
    public Collection<TopK.Bucket> getBuckets() {
        return this.buckets;
    }

    @Override
    public Type type() {
        return TYPE;
    }
    
    @Override
    public InternalTopK reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        InternalTopK reduced = null;
        if (aggregations.size() == 1) {
            reduced = ((InternalTopK) aggregations.get(0));
            for (TopK.Bucket bucket : reduced.getBuckets()) {
                ((InternalAggregations) bucket.getAggregations()).reduce(reduceContext.bigArrays());
            }
            return reduced;
        }
        HashMap<String, List<TopK.Bucket>> termToBucket = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalTopK topk = (InternalTopK) aggregation;
            if (reduced == null) {
                reduced = topk;
            } else {
                StreamSummary<String> s = topk.summary;
                if (s != null) {
                    if (reduced.summary == null) {
                        reduced.summary = s;
                    } else {
                        for (TopK.Bucket bucket : topk.getBuckets()) {
                            reduced.summary.offer(bucket.getKey(), (int) bucket.getDocCount());
                        }
                    }
                }
            }
            for (TopK.Bucket bucket : topk.getBuckets()) {
                List<TopK.Bucket> buckets = termToBucket.get(bucket.getKey());
                if (buckets == null) {
                    buckets = new ArrayList<>();
                    termToBucket.put(bucket.getKey(), buckets);
                }
                buckets.add(bucket);
            }
        }
        reduced.buckets.clear();
        reduced.bucketsMap = null;
        List<Counter<String>> counters = reduced.summary.topK(reduced.size.intValue());
        int bucketOrd = 0;
        for (Counter<String> c : counters) {
            List<InternalAggregations> aggs = new ArrayList<>();
            for (TopK.Bucket bucket : termToBucket.get(c.getItem())) {
                aggs.add(bucket.aggregations);
            }
            reduced.buckets.add(new TopK.Bucket(c.getItem(), c.getCount(), bucketOrd++, InternalAggregations.reduce(aggs, reduceContext.bigArrays())));
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        size = in.readInt();
        int n = in.readInt();
        byte[] bytes = new byte[n];
        in.read(bytes);
        try {
            summary.fromBytes(bytes);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to build summary", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        out.writeInt(this.size.intValue());
        byte[] bytes = this.summary.toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startArray(CommonFields.BUCKETS);
        for (TopK.Bucket bucket : getBuckets()) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
