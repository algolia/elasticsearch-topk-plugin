**Disclaimer:** While we're **not using ElasticSearch** for [Algolia](http://www.algolia.com)'s hosted full-text, numerical & faceted search engine; we're using it for internal analytics (faceting over billions of API calls, no FTS).

Elasticsearch Top-K Plugin
===================================

This plugin extends Elasticsearch providing a fast & memory-efficient way to get the Top-K elements of a field through an aggregation. The field can be either string, numerical or boolean. The plugin registers a new type of aggregation (`topk`).

We _love_ pull-requests!

### Prerequisites:

 - Elasticsearch 1.2.0+

### Binaries

 - Compiled versions of the plugin are stored in the [`dist`](https://github.com/algolia/elasticsearch-topk-plugin/tree/master/dist) directory.

## Principle

This plugin uses the ```StreamSummary``` data structure provided by the [Stream-lib](https://github.com/addthis/stream-lib) library to get the top-k terms of a field. Basically, it computes the most frequent terms of a field without loading all of them into RAM. The merge between shards and between indices is supported (and efficient).

## Usage

To build an aggregation keeping the top-k elements of a field, use the following code:

```json
{
  "aggregations": {
    "<aggregation_name>": {
      "topk": {
        "field": "<field_name>",
        "size": 10
      }
    }
  }
}
```

For example, to keep the 10 most frequent values of your "ip" field, use:

```json
{
  "aggregations": {
    "top_ips": {
      "topk": {
        "field": "ip",
        "size": 100
      }
    }
  }
}
```

```json
{
  "aggregations": {
    "top_ips": {
      "buckets": [
        { "key": "1.2.3.4", "doc_count": 42 },
        { "key": "5.6.7.8", "doc_count": 1 },
      ]
    }
  }
}
```

## Setup

### Installation 

    ./plugin --url file:///absolute/path/to/elasticsearch-topk-plugin-0.0.1.zip --install topk-aggregation

### Uninstallation

    ./plugin --remove topk-aggregation
