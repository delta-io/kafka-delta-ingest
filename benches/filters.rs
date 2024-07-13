use std::fs::File;
use std::io::{self, BufRead, BufReader};

use criterion::{black_box, Criterion, criterion_group, criterion_main};
use serde_json::Value;

use kafka_delta_ingest::{Filter, FilterEngine, FilterError, FilterFactory};

const SOURCE_PATH: &str = "tests/json/web_requests-100.json";

fn read_json_file(file_path: &str) -> io::Result<Vec<Value>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let lines: Vec<_> = reader.lines().take(30000).collect::<io::Result<_>>()?;

    let values: Vec<Value> = lines
        .iter()
        .map(|line| serde_json::from_str::<Value>(&line).unwrap())
        .collect();

    Ok(values)
}

fn filtering(filter: &Box<dyn Filter>, values: &Vec<Value>) {
    for v in values.into_iter() {
        match filter.filter(v) {
            Ok(_) => {}
            Err(e) => {
                match e {
                    FilterError::FilterSkipMessage => {}
                    _ => panic!("something wrong"),
                }
            }
        };
    }
}

fn naive_filter_benchmark(c: &mut Criterion) {
    let values = read_json_file(SOURCE_PATH).unwrap();
    let filter = FilterFactory::try_build(&FilterEngine::Naive, &vec!("method=='GET'".to_string())).expect("wrong");
    c.bench_function("naive_filter_benchmark", |b| {
        b.iter(|| filtering(&filter, black_box(&values)))
    });
}


fn jmespath_filter_benchmark(c: &mut Criterion) {
    let values = read_json_file(SOURCE_PATH).unwrap();
    let filter = FilterFactory::try_build(&FilterEngine::Jmespath, &vec!("method=='GET'".to_string())).expect("wrong");
    c.bench_function("jmespath_filter_benchmark", |b| {
        b.iter(|| filtering(&filter, black_box(&values)))
    });
}


criterion_group!(benches, naive_filter_benchmark, jmespath_filter_benchmark);
criterion_main!(benches);
