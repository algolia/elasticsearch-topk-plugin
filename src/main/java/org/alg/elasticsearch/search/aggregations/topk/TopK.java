package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;
import java.util.Collection;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
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
        final int bucketOrd;
        InternalAggregations aggregations;
        
        public Bucket(String term, long count, int bucketOrd, InternalAggregations aggregations) {
            this.term = term;
            this.count = count;
            this.bucketOrd = bucketOrd;
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
        
        void toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, term);
            builder.field(CommonFields.DOC_COUNT, count);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }
        
        void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeLong(count);
            out.writeInt(bucketOrd);
            if (aggregations != null) {
                out.writeBoolean(true);
                aggregations.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
        
        static TopK.Bucket readFrom(StreamInput in) throws IOException {
            String term = in.readString();
            long count = in.readLong();
            int bucketOrd = in.readInt();
            return new TopK.Bucket(term, count, bucketOrd, InternalAggregations.readOptionalAggregations(in));
        }
    }
    
    Collection<Bucket> getBuckets();

    @SuppressWarnings("unchecked")
    Bucket getBucketByKey(String term);
}
