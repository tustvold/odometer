# DynamoDB Odometer

A simple binary for testing the replication latency of DynamoDB Streams.

```
$ cargo run --release TABLE_NAME PRIMARY_KEY_NAME
    ...
    
Reading stream ... with 4 shards
Fetched shard iterators
Took 6.424079452s to process 1000 records with a concurrency of 10
With an average record latency of 79.698718082ms
```
