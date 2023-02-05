CREATE OR REPLACE PROCEDURE staging.update_fact_vehicle_location()
LANGUAGE plpgsql 
AS $$
BEGIN

    -- insert into fact vehicle location
    INSERT INTO dm.fact_vehicle_location(vehicle_sid, lat, lng, record_timestamp)
    SELECT DISTINCT
        dim.s_id,
        stg.lat, 
        stg.lng,
        stg.record_timestamp
    FROM staging.stg_vehicle_location stg
    INNER JOIN dm.dim_vehicle dim
        on stg.id = dim.id 
    WHERE NOT EXISTS(
        SELECT 1
        FROM dm.fact_vehicle_location sub_dim
        WHERE 
            sub_dim.s_id = dim.s_id
            AND sub_dim.lat = stg.lat
            AND sub_dim.lng = stg.lng
            AND sub_dim.record_timestamp = stg.record_timestamp
    );

END; $$
