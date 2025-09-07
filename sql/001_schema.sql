
CREATE TABLE IF NOT EXISTS raw_carpark_current_availability (
  ingest_ts_sgt      TIMESTAMP NOT NULL,
  carpark_number     TEXT NOT NULL,
  total_lots         INTEGER,
  lot_type           TEXT,
  available_lots     INTEGER,
  update_datetime_sg TIMESTAMP,
  payload_json       JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_carpark_info (
  car_park_no            TEXT PRIMARY KEY,
  address                TEXT,
  x_coord                TEXT,
  y_coord                TEXT,
  car_park_type          TEXT,
  type_of_parking_system TEXT,
  short_term_parking     TEXT,
  free_parking           TEXT,
  night_parking          TEXT,
  car_park_decks         TEXT,
  gantry_height          TEXT,
  car_park_basement      TEXT
);

CREATE TABLE IF NOT EXISTS raw_carpark_availability_6pm_last_30days (
  ingest_ts_sgt      TIMESTAMP NOT NULL,
  carpark_number     TEXT NOT NULL,
  total_lots         INTEGER,
  lot_type           TEXT,
  available_lots     INTEGER,
  update_datetime_sg TIMESTAMP,
  payload_json       JSONB NOT NULL,
  PRIMARY KEY (update_datetime_sg, carpark_number)
);
