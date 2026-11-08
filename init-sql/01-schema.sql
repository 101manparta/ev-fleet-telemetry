-- Skema database untuk Fleet Telemetry & Diagnostik Mobil Listrik
-- Dijalankan otomatis oleh PostgreSQL saat container pertama kali nyala

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    plate_number VARCHAR(20) NOT NULL,
    model VARCHAR(100),
    battery_capacity_kwh NUMERIC(6,2),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS telemetry (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL REFERENCES vehicles(vehicle_id),
    timestamp TIMESTAMP NOT NULL,
    soc_percent NUMERIC(5,2),        -- State of Charge baterai (%)
    battery_temp_c NUMERIC(5,2),     -- Suhu baterai (Celcius)
    speed_kmh NUMERIC(6,2),          -- Kecepatan (km/jam)
    charger_connected BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_telemetry_vehicle_time ON telemetry(vehicle_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS diagnostics (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL REFERENCES vehicles(vehicle_id),
    alert_type VARCHAR(50) NOT NULL,     -- contoh: soc_critical, battery_overheat
    severity VARCHAR(20) CHECK (severity IN ('warning','critical')),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP
);

CREATE INDEX idx_diagnostics_vehicle ON diagnostics(vehicle_id, detected_at DESC);

-- Data contoh armada
INSERT INTO vehicles (vehicle_id, plate_number, model, battery_capacity_kwh) VALUES
    ('EV-001', 'B 1234 XYZ', 'Tesla Model 3', 75.0),
    ('EV-002', 'B 5678 ABC', 'Wuling Air EV', 32.0),
    ('EV-003', 'B 9012 DEF', 'Hyundai Ioniq 5', 77.4)
ON CONFLICT (vehicle_id) DO NOTHING;