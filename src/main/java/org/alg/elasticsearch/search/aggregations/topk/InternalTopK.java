package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

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
    private final Number size;

    InternalTopK() {  // for serialization
        this.size = null;
        this.summary = null;
    }

    InternalTopK(String name, Number size, StreamSummary<String> summary) {
        super(name);
        this.size = size;
        this.summary = summary;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TopK.Bucket getBucketByKey(String term) {
        List<Counter<String>> counters = this.summary.topK(this.size.intValue());
        for (Counter<String> c : counters) {
            if (c.getItem().equals(term)) {
                new TopK.Bucket(c.getItem(), c.getCount(), null);
            }
        }
        throw new NoSuchElementException("Cannot find term '" + term + "'");
    }
    
    @Override
    public Collection<TopK.Bucket> getBuckets() {
        List<TopK.Bucket> buckets = new ArrayList<>(this.size.intValue());
        if (this.summary != null) {
            List<Counter<String>> counters = this.summary.topK(this.size.intValue());
            for (Counter<String> c : counters) {
                buckets.add(new TopK.Bucket(c.getItem(), c.getCount(), null));
            }
        }
        return buckets;
    }

    @Override
    public Type type() {
        return TYPE;
    }
    
    @Override
    public InternalTopK reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        InternalTopK reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalTopK) aggregation;
            } else {
                StreamSummary<String> s = ((InternalTopK) aggregation).summary;
                if (s != null) {
                    if (reduced.summary == null) {
                        reduced.summary = s;
                    } else {
                        for (Counter<String> c : reduced.summary.topK(size.intValue())) {
                            reduced.summary.offer(c.getItem(), (int) c.getCount());
                        }
                    }
                }
            }
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        if (in.readBoolean()) {
            int n = in.readInt();
            byte[] bytes = new byte[n];
            in.read(bytes);
            try {
                summary.fromBytes(bytes);
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to build summary", e);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (summary != null) {
            out.writeBoolean(true);
            byte[] bytes = summary.toBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            out.writeBoolean(false);
        }
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
