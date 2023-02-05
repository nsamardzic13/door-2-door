CREATE OR REPLACE PROCEDURE staging.update_dim_operating_period()
LANGUAGE plpgsql 
AS $$
BEGIN

    -- insert new vehicles
    INSERT INTO dm.dim_operating_period(id, org_id, period_start, period_finish, record_timestamp, is_active)
    SELECT DISTINCT
        id,
        org_id,
        period_start,
        period_finish,
        record_timestamp,
        CASE 
            WHEN event_type = 'create' THEN true
            ELSE false
        END as is_active
    FROM staging.stg_operatin_period stg
    WHERE NOT EXISTS (
        SELECT 1
        FROM dm.dim_operating_period dim
        WHERE 
            dim.id = stg.id
            AND dim.org_id = stg.org_id
            AND dim.period_start = stg.period_start
            AND dim.period_finish = stg.period_finish
            AND dim.record_timestamp = stg.record_timestamp
            AND dim.is_active = CASE 
                                WHEN stg.event_type = 'create' THEN true
                                ELSE false
                            END 
    );

END; $$