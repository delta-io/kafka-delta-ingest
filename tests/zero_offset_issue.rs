use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[allow(dead_code)]
mod helpers;

#[derive(Debug, Serialize, Deserialize)]
struct TestMsg {
    id: u32,
    color: String,
}

impl TestMsg {
    fn new(id: u32) -> Self {
        Self {
            id,
            color: "default".to_string(),
        }
    }
}

#[tokio::test]
async fn zero_offset_issue() {
    let table = "./tests/data/zero_offset";
    helpers::init_logger();
    let topic = format!("zero_offset_issue_{}", Uuid::new_v4());

    helpers::create_topic(&topic, 1).await;

    let mut kdi = helpers::create_kdi("zero_offset", &topic, table, 5, 1, 20);
    let rt = helpers::create_runtime("zero_offset_L");

    let token = Arc::new(CancellationToken::new());
    let run_loop = {
        let token = token.clone();
        rt.spawn(async move { kdi.start(Some(&token)).await.unwrap() })
    };

    {
        // check that there's only 1 record in table
        let table = deltalake::open_table(table).await.unwrap();
        assert_eq!(table.version, 1);
        assert_eq!(count_records(table), 1);
    }

    let producer = helpers::create_producer();

    // submit 3 messages in kafka, but only 2nd and 3rd should go in as msg 0:0 already in delta
    for i in 0..3 {
        helpers::send_json(
            &producer,
            &topic,
            &serde_json::to_value(TestMsg::new(i)).unwrap(),
        )
        .await;
    }

    let v2 = Path::new("./tests/data/zero_offset/_delta_log/00000000000000000002.json");
    let v3 = Path::new("./tests/data/zero_offset/_delta_log/00000000000000000003.json");

    wait_until_created(v2);
    wait_until_created(v3);
    token.cancel();
    // if it succeeds then it means that we successfully seeked into offset 0, e.g. Offset::Beginning
    run_loop.await.unwrap();
    rt.shutdown_background();

    // check that there's only 3 records
    let table = deltalake::open_table(table).await.unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(count_records(table), 3);

    //cleanup
    std::fs::remove_file(v2).unwrap();
    std::fs::remove_file(v3).unwrap();
}

fn wait_until_created(path: &Path) {
    loop {
        if path.exists() {
            return;
        }
    }
}

fn count_records(table: DeltaTable) -> i64 {
    let mut count = 0;
    for x in table.get_stats().iter() {
        count += x.as_ref().unwrap().as_ref().unwrap().num_records;
    }
    count
}
