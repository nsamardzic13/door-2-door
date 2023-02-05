create table if not exists staging.stg_vehicle_location(
    id                      varchar(40),
    org_id                  varchar(40),
    lat                     float,
    lng                     float,
    record_timestamp        timestamp
);

create table if not exists staging.stg_vehicle_register(
    id                      varchar(40),
    org_id                  varchar(40),
    event_type              varchar(12),
    record_timestamp        timestamp
);

create table if not exists staging.stg_operatin_period(
    id                      varchar(40),
    org_id                  varchar(40),
    period_start            timestamp,
    period_finish           timestamp,
    event_type              varchar(10),
    record_timestamp        timestamp
);

create table if not exists dm.dim_vehicle(
    s_id                    serial PRIMARY KEY,
    id                      varchar(40),
    org_id                  varchar(40),
    record_timestamp        timestamp,
    is_active               boolean
);

create table if not exists dm.dim_operating_period(
    s_id                    serial PRIMARY KEY,
    id                      varchar(40),
    org_id                  varchar(40),
    period_start            timestamp,
    period_finish           timestamp,
    record_timestamp        timestamp,
    is_active               boolean
);

create table if not exists dm.fact_vehicle_location(
    s_id                    serial PRIMARY KEY,
    vehicle_sid             integer,
    lat                     float,
    lng                     float,
    record_timestamp        timestamp
);