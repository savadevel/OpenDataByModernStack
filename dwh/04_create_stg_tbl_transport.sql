CREATE TABLE IF NOT EXISTS stg.tbl_transport (
    id SERIAL PRIMARY KEY,
    int_id INT NOT NULL,
    year INT NOT NULL,
    transport_type VARCHAR NOT NULL,
    passenger_traffic BIGINT NOT NULL,
    global_id BIGINT NOT NULL,
    loaded TIMESTAMP NOT NULL DEFAULT NOW());