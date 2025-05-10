use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use env_logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();

    // Kafka consumer configuration
    let brokers = "localhost:9092";
    let group_id = "audit_log";
    let topic = "test";

    // Create consumer
    let mut consumer = Consumer::from_hosts(vec![brokers.to_owned()])
        .with_topic(topic.to_owned())
        .with_group(group_id.to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .unwrap();

    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                println!("{:?}", m);
            }
            let _ = consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
        }
}
