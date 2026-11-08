use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use tokio_postgres::NoTls;
use std::env;

#[derive(Deserialize, Debug)]
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

    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://evadmin:evpassword@localhost:5432/ev_fleet".to_string());
    let brokers = env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "ev-telemetry".to_string());

    // Koneksi ke PostgreSQL
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ DB connection error: {}", e);
        }
    });

    // Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "ev-fleet-diagnostic")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[&topic])?;

    println!("📡 EV Consumer started.");
    println!("   Brokers: {}", brokers);
    println!("   Topic:   {}", topic);
    println!("   Database: terhubung");
    println!("   Tekan Ctrl+C untuk berhenti.\n");

    loop {
        match consumer.recv().await {
            Some(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<Telemetry>(payload) {
                        Ok(telemetry) => {
                            if let Err(e) = handle_telemetry(&client, &telemetry).await {
                                eprintln!("❌ Gagal proses data: {}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Gagal parse JSON: {}", e);
                        }
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("❌ Kafka recv error: {}", e);
            }
            None => continue,
        }
    }
}

async fn handle_telemetry(
    client: &tokio_postgres::Client,
    t: &Telemetry,
) -> Result<(), Box<dyn std::error::Error>> {
    // Simpan data sensor ke tabel telemetry
    client.execute(
        "INSERT INTO telemetry (vehicle_id, timestamp, soc_percent, battery_temp_c, speed_kmh, charger_connected)
         VALUES ($1, to_timestamp($2)::timestamp, $3, $4, $5, $6)",
        &[
            &t.vehicle_id,
            &(t.timestamp as f64),
            &t.soc_percent,
            &t.battery_temp_c,
            &t.speed_kmh,
            &t.charger_connected,
        ],
    ).await?;

    // Deteksi anomali baterai
    if t.soc_percent < 20.0 {
        insert_alert(client, &t.vehicle_id, "soc_critical", "critical").await?;
    }
    if t.battery_temp_c > 45.0 {
        insert_alert(client, &t.vehicle_id, "battery_overheat", "critical").await?;
    } else if t.battery_temp_c > 40.0 {
        insert_alert(client, &t.vehicle_id, "battery_warm", "warning").await?;
    }

    println!(
        "✅ {} | SoC: {:.1}% | Temp: {:.1}°C | Speed: {:.1} km/h | Charger: {}",
        t.vehicle_id, t.soc_percent, t.battery_temp_c, t.speed_kmh, t.charger_connected
    );

    Ok(())
}

async fn insert_alert(
    client: &tokio_postgres::Client,
    vehicle_id: &str,
    alert_type: &str,
    severity: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client.execute(
        "INSERT INTO diagnostics (vehicle_id, alert_type, severity) VALUES ($1, $2, $3)",
        &[&vehicle_id, &alert_type, &severity],
    ).await?;
    println!(
        "🚨 ALERT [{}] {}: {}",
        severity.to_uppercase(),
        vehicle_id,
        alert_type
    );
    Ok(())
}