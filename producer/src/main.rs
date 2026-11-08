use rdkafka::config::ClientConfig;
use rdkafka::producer::{Producer, FutureProducer};
use rdkafka::message::FutureRecord;
use serde::Serialize;
use serde_json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use std::time::Duration;

#[derive(Serialize, Clone)]
struct Telemetry {
    vehicle_id: String,
    timestamp: i64,
    soc_percent: f32,
    battery_temp_c: f32,
    speed_kmh: f32,
    charger_connected: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "ev-telemetry".to_string());

    let vehicles: &[(&str, f32)] = &[
        ("EV-001", 75.0),
        ("EV-002", 32.0),
        ("EV-003", 77.4),
    ];

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    println!("🚗 EV Producer started.");
    println!("   Brokers: {}", brokers);
    println!("   Topic:   {}", topic);
    println!("   Armada : {} mobil", vehicles.len());
    println!("   Tekan Ctrl+C untuk berhenti.\n");

    let mut counter: u64 = 0;

    loop {
        counter += 1;

        for (vid, capacity) in vehicles.iter() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            // Simulasi data sensor sederhana (berubah-ubah tiap detik)
            let soc = 80.0 - ((counter as f32) % 60.0);
            let temp = 35.0 + ((counter as f32) % 15.0);
            let speed = (counter as f32 % 90.0);
            let charging = counter % 3 == 0;

            let telemetry = Telemetry {
                vehicle_id: vid.to_string(),
                timestamp: now,
                soc_percent: soc,
                battery_temp_c: temp,
                speed_kmh: speed,
                charger_connected: charging,
            };

            let payload = serde_json::to_string(&telemetry)?;
            let key = vid.to_string();

            producer
                .send(
                    FutureRecord::to(&topic)
                        .payload(&payload)
                        .key(&key),
                    Duration::from_secs(0),
                )
                .await?;

            println!(
                "📤 {} | SoC: {:.1}% | Temp: {:.1}°C | Speed: {:.1} km/h | Charger: {}",
                vid, soc, temp, speed, charging
            );
        }

        sleep(Duration::from_secs(2)).await;
    }
}