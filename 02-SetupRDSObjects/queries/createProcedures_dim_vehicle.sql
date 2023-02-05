CREATE OR REPLACE PROCEDURE staging.update_dim_vehicle()
LANGUAGE plpgsql 
AS $$
BEGIN

    -- insert new vehicles
    INSERT INTO dm.dim_vehicle(id, org_id, record_timestamp, is_active)
    SELECT DISTINCT
        id,
        org_id,
        record_timestamp,
        CASE 
            WHEN event_type = 'register' THEN true
            ELSE false
        END as is_active
    FROM staging.stg_vehicle_register stg
    WHERE NOT EXISTS (
        SELECT 1
        FROM dm.dim_vehicle dim
        WHERE 
            dim.id = stg.id
            AND dim.org_id = stg.org_id
            AND dim.record_timestamp = stg.record_timestamp
            AND dim.is_active = CASE 
                        WHEN stg.event_type = 'register' THEN true
                        ELSE false
                    END
    );

END; $$