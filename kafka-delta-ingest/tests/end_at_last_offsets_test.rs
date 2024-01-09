use deltalake::DeltaTable;
use kafka_delta_ingest::IngestOptions;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[allow(dead_code)]
mod helpers;

#[derive(Debug, Serialize, Deserialize)]
struct Msg {
    id: u32,
    city: String,
}

impl Msg {
    fn new(id: u32) -> Self {
        Self {
            id,
            city: "default".to_string(),
        }
    }
}

#[tokio::test]
async fn end_at_initial_offsets() {
    helpers::init_logger();
    let topic = format!("end_at_offset_{}", Uuid::new_v4());

    let table = helpers::create_local_table(
        json!({
            "id": "integer",
            "city": "string",
        }),
        vec!["city"],
        &topic,
    );
    let table = table.as_str();

    helpers::create_topic(&topic, 3).await;

    let producer = helpers::create_producer();
    // submit 15 messages in kafka
    for i in 0..15 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(Msg::new(i)).unwrap(),
        )
        .await;
    }

    let (kdi, _token, rt) = helpers::create_kdi(
        &topic,
        table,
        IngestOptions {
            app_id: topic.clone(),
            allowed_latency: 5,
            max_messages_per_batch: 20,
            min_bytes_per_file: 20,
            end_at_last_offsets: true,
            ..Default::default()
        },
    );

    helpers::wait_until_version_created(table, 1);

    {
        // check that there's 3 records in table
        let table = deltalake::open_table(table).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(count_records(table), 15);
    }

    // messages in kafka
    for i in 16..31 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(Msg::new(i)).unwrap(),
        )
        .await;
    }

    helpers::expect_termination_within(kdi, 10).await;
    rt.shutdown_background();

    // check that there's only 3 records
    let table = deltalake::open_table(table).await.unwrap();
    assert_eq!(table.version(), 1);
    assert_eq!(count_records(table), 15);
}

fn count_records(table: DeltaTable) -> i64 {
    let mut count = 0;
    for x in table.get_stats() {
        count += x.as_ref().unwrap().as_ref().unwrap().num_records;
    }
    count
}
