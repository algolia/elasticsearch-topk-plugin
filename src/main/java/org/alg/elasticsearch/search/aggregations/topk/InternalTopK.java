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
        if (this.summary != null) {
            List<Counter<String>> counters = this.summary.topK(this.size.intValue());
            for (Counter<String> c : counters) {
                this.buckets.add(new TopK.Bucket(c.getItem(), c.getCount(), null));
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
        StreamSummary<String> summary = null;
        for (InternalAggregation aggregation : aggregations) {
            if (summary == null) {
                summary = new StreamSummary<>(((InternalTopK) aggregation).size.intValue());
            }
            StreamSummary<String> s = ((InternalTopK) aggregation).summary;
            if (s != null) {
                for (Counter<String> c : s.topK(this.size.intValue() * aggregations.size())) {
                    summary.offer(c.getItem(), (int) c.getCount());
                }
            }
        }
        InternalTopK first = ((InternalTopK) aggregations.get(0));
        return new InternalTopK(first.name, first.size, summary);
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
        List<TopK.Bucket> buckets = new ArrayList<>(size.intValue());
        for (Counter<String> c : summary.topK(size.intValue())) {
            buckets.add(new TopK.Bucket(c.getItem(), c.getCount(), null));
        }
        this.buckets = buckets;
        this.bucketsMap = null;
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
            builder.startObject();
            builder.field(CommonFields.KEY, ((Bucket) bucket).getKey());
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
