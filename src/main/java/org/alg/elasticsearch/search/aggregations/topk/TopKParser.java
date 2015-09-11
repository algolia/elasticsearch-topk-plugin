package org.alg.elasticsearch.search.aggregations.topk;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class TopKParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalTopK.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceConfig<ValuesSource.Bytes> config = new ValuesSourceConfig<>(ValuesSource.Bytes.class);
        
        String field = null;
        Number size = null;
        Number capacity = 1000;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    size = parser.numberValue();
                } else if ("capacity".equals(currentFieldName)) {
                        capacity = parser.numberValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (field == null) {
            throw new SearchParseException(context, "Key 'field' cannot be null.");
        }
        if (size == null) {
            throw new SearchParseException(context, "Key 'size' cannot be null.");
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new TopKAggregator.Factory(aggregationName, config, size, capacity);
        }
        config.fieldContext(new FieldContext(field, context.fieldData().getForField(mapper), mapper));
        return new TopKAggregator.Factory(aggregationName, config, size, capacity);
    }
}
