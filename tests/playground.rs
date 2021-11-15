use kafka_delta_ingest::IngestOptions;
use log::info;
use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[allow(dead_code)]
mod helpers;

// This is not an actual test but rather a playground to test various things and features
// with a minimal effort. Just edit the `Playground::run_custom_checks` function below
// however you need and execute the `RUST_LOG=INFO cargo test playground`.
// The table content is in `TABLE_PATH`, and by default it's cleared before each run.
#[tokio::test]
async fn playground() {
    Playground::run().await;
}

const TABLE_PATH: &str = "./tests/data/gen/playground";
const MAX_MESSAGES_PER_BATCH: usize = 1;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Message {
    id: u32,
    date: Option<String>,
}

impl Message {
    pub fn new(id: u32, date: Option<String>) -> Self {
        Self { id, date }
    }
}

struct Playground {
    producer: FutureProducer,
    topic: String,
}

impl Playground {
    async fn run_custom_checks(&self) {
        // Run custom checks here!

        for id in 1..25 {
            self.send_message(Message::new(id, Some("p".to_string())))
                .await;
        }

        helpers::wait_until_version_created(TABLE_PATH, 24);
    }

    async fn send_message(&self, msg: Message) {
        self.send_json(&serde_json::to_value(msg).unwrap()).await;
    }

    async fn send_json(&self, value: &Value) {
        helpers::send_json(&self.producer, &self.topic, value).await;
    }

    async fn run() {
        helpers::init_logger();
        let _ = std::fs::remove_dir_all(TABLE_PATH);
        helpers::create_local_table_in(
            json!({
                "id": "integer",
                "date": "string",
            }),
            vec!["date"],
            TABLE_PATH,
        );

        let topic = format!("playground_{}", Uuid::new_v4());
        helpers::create_topic(&topic, 1).await;

        let ingest_options = IngestOptions {
            app_id: "schema_update".to_string(),
            allowed_latency: 5,
            max_messages_per_batch: MAX_MESSAGES_PER_BATCH,
            min_bytes_per_file: 20,
            ..Default::default()
        };

        let (kdi, token, rt) = helpers::create_kdi(&topic, TABLE_PATH, ingest_options);
        let producer = helpers::create_producer();
        let playground = Playground {
            producer,
            topic: topic.clone(),
        };

        playground.run_custom_checks().await;

        token.cancel();
        playground.send_json(&Value::Null).await;
        kdi.await.unwrap();
        rt.shutdown_background();

        info!("The table {} content:", TABLE_PATH);
        let content = helpers::read_table_content_as_jsons(TABLE_PATH).await;
        let len = content.len();
        let mut bytes = 0;
        for json in content {
            bytes += json.to_string().as_bytes().len();
            info!("{}", json);
        }
        let avg = bytes as f64 / len as f64;
        info!(
            "Total {} records and {} bytes. Average message size: {:.2} bytes",
            len, bytes, avg
        );
        helpers::delete_topic(&topic).await;
    }
}
