use anyhow::{anyhow, bail, Context, Error, Ok};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodbstreams::types::{OperationType, ShardIteratorType};
use clap::Parser;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// DynamoDB table arn
    table: String,

    /// The primary key
    key: String,

    /// Number of writes to perform
    #[arg(short, long, default_value_t = 1000)]
    iterations: usize,

    /// Number of concurrent writes to perform
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        table,
        key,
        iterations,
        concurrency,
    } = Args::parse();

    let config = aws_config::load_from_env().await;
    let dynamo = aws_sdk_dynamodb::Client::new(&config);

    let describe = dynamo.describe_table().table_name(&table).send().await?;
    let table_desc = describe.table.ok_or_else(|| anyhow!("missing table"))?;
    let stream_arn = table_desc
        .latest_stream_arn
        .ok_or_else(|| anyhow!("DynamoDB streams not enabled"))?;

    let streams = aws_sdk_dynamodbstreams::Client::new(&config);

    let stream = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await?;

    let stream_desc = stream
        .stream_description
        .ok_or_else(|| anyhow!("empty stream description"))?;

    let shards = stream_desc
        .shards
        .ok_or_else(|| anyhow!("empty stream shards"))?;

    println!("Reading stream {stream_arn} with {} shards", shards.len());

    // Fetch the shard iterators before writing new data
    let shard_iterators: Vec<_> = futures::stream::iter(shards)
        .map(|shard| async {
            let id = shard.shard_id.ok_or_else(|| anyhow!("missing shard ID"))?;

            let shard = streams
                .get_shard_iterator()
                .stream_arn(&stream_arn)
                .shard_id(id)
                .shard_iterator_type(ShardIteratorType::Latest)
                .send()
                .await?;
            shard
                .shard_iterator
                .ok_or_else(|| anyhow!("missing shard iterator"))
        })
        .buffered(concurrency)
        .try_collect()
        .await?;

    println!("Fetched shard iterators");

    let start = Instant::now();

    let recv = async {
        let shard_streams = shard_iterators.into_iter().map(|iterator| {
            futures::stream::try_unfold(iterator, |iterator| async {
                let resp = streams
                    .get_records()
                    .shard_iterator(iterator)
                    .send()
                    .await?;

                let next = resp
                    .next_shard_iterator
                    .ok_or_else(|| anyhow!("missing next shard iterator"))?;

                let records = resp.records.ok_or_else(|| anyhow!("missing records"))?;
                let records_stream = futures::stream::iter(records).map(Ok);
                Ok(Some((records_stream, next)))
            })
            .try_flatten()
            .boxed()
        });

        let key = &key;
        futures::stream::select_all(shard_streams)
            .and_then(|record| async move {
                if !matches!(record.event_name, Some(OperationType::Modify)) {
                    bail!("unexpected event {:?}", record.event_name)
                }
                let record = record
                    .dynamodb
                    .ok_or_else(|| anyhow!("missing event record"))?;

                let keys = record.keys.ok_or_else(|| anyhow!("missing record keys"))?;
                let key: usize = match keys.get(key) {
                    Some(aws_sdk_dynamodbstreams::types::AttributeValue::N(s)) => {
                        s.parse().context("parsing key")?
                    }
                    _ => bail!("missing key attribute"),
                };

                Ok((key, Instant::now())) // Record the time received
            })
            .take(iterations)
            .try_collect::<Vec<_>>()
            .await
    };

    // A unique UUID for this run
    let run_id = Uuid::new_v4().to_string();

    let send = async {
        futures::stream::iter(0..iterations)
            .map(|x| {
                dynamo
                    .put_item()
                    .table_name(&table)
                    .item(&key, AttributeValue::N(x.to_string()))
                    .item("run", AttributeValue::S(run_id.clone()))
                    .send()
                    .map_ok(|_| Instant::now()) // Record the time of the write
                    .map_err(Error::from)
            })
            .buffered(concurrency)
            .try_collect::<Vec<_>>()
            .await
    };

    let (send_times, mut recv_times) = futures::future::try_join(send, recv).await?;
    recv_times.sort_unstable_by_key(|(key, _)| *key);

    let total_latency: Duration = send_times
        .iter()
        .zip(&recv_times)
        .map(|(send, (_, recv))| *recv - *send)
        .sum();
    let avg_latency = total_latency.as_secs_f64() / iterations as f64 * 1000.;

    let elapsed = start.elapsed().as_secs_f64();
    println!("Took {elapsed}s to process {iterations} records with a concurrency of {concurrency}");
    println!("With an average record latency of {avg_latency}ms",);

    Ok(())
}
