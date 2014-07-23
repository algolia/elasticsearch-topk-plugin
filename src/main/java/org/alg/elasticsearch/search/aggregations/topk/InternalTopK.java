package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.alg.elasticsearch.search.aggregations.topk.TopKAggregator.Term;
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

    private StreamSummary<Term> summary;
    private Number size;
    private List<TopK.Bucket> buckets;
    private HashMap<String, TopK.Bucket> bucketsMap;

    InternalTopK() { }  // for serialization

    InternalTopK(String name, Number size, StreamSummary<Term> summary) {
        super(name);
        this.size = size;
        this.summary = summary;
        this.buckets = new ArrayList<>();
        this.bucketsMap = null;
        if (this.summary != null) {
            List<Counter<Term>> counters = this.summary.topK(this.size.intValue());
            for (Counter<Term> c : counters) {
                this.buckets.add(new TopK.Bucket(c.getItem().term, c.getCount(), c.getItem().bucketOrd, null));
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TopK.Bucket getBucketByKey(String term) {
        if (bucketsMap == null) {
            bucketsMap = new HashMap<>();
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
        
        // single aggregation
        if (aggregations.size() == 1) {
            reduced = ((InternalTopK) aggregations.get(0));
            for (TopK.Bucket bucket : reduced.getBuckets()) {
                ((InternalAggregations) bucket.getAggregations()).reduce(reduceContext.bigArrays());
            }
            return reduced;
        }

        // reduce all top-k
        HashMap<String, List<TopK.Bucket>> termToBucket = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalTopK topk = (InternalTopK) aggregation;

            // build term->(bucket*) dict
            for (TopK.Bucket bucket : topk.getBuckets()) {
                List<TopK.Bucket> buckets = termToBucket.get(bucket.getKey());
                if (buckets == null) {
                    buckets = new ArrayList<>();
                    termToBucket.put(bucket.getKey(), buckets);
                }
                buckets.add(bucket);
            }

            // merge top-k terms
            if (reduced == null) {
                reduced = topk;
            } else {
                StreamSummary<Term> s = topk.summary;
                if (s != null) {
                    if (reduced.summary == null) {
                        reduced.summary = s;
                    } else {
                        // TODO: we're building the reduced top-k with all other top-k, there's probably room for improvement
                        for (TopK.Bucket bucket : topk.getBuckets()) {
                            reduced.summary.offer(new Term(bucket.getKey(), bucket.bucketOrd), (int) bucket.getDocCount());
                        }
                    }
                }
            }
        }
        
        // rebuild buckets
        reduced.buckets.clear();
        reduced.bucketsMap = null;
        if (reduced.summary != null) {
            List<Counter<Term>> counters = reduced.summary.topK(reduced.size.intValue());
            int bucketOrd = 0;
            for (Counter<Term> c : counters) {
                List<InternalAggregations> aggs = new ArrayList<>();
                for (TopK.Bucket bucket : termToBucket.get(c.getItem().term)) {
                    aggs.add(bucket.aggregations);
                }
                reduced.buckets.add(new TopK.Bucket(c.getItem().term, c.getCount(), bucketOrd++, InternalAggregations.reduce(aggs, reduceContext.bigArrays())));
            }
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        size = in.readInt();
        int capacity = in.readInt();
        this.summary = new StreamSummary<>(capacity);
        int n = in.readInt();
        byte[] bytes = new byte[n];
        in.read(bytes);
        try {
            summary.fromBytes(bytes);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to build summary", e);
        }
        n = in.readInt();
        this.buckets = new ArrayList<>();
        this.bucketsMap = null;
        for (int i = 0; i < n; ++i) {
            buckets.add(Bucket.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        out.writeInt(this.size.intValue());
        out.writeInt(this.summary.getCapacity());
        byte[] bytes = this.summary.toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
        out.writeInt(getBuckets().size());
        for (TopK.Bucket bucket : getBuckets()) {
            bucket.writeTo(out);
        }
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
