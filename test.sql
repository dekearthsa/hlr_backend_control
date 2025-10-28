SELECT 
    minute_th,
    cyclicName,
    CASE
        WHEN sensor_id = 2 THEN avg_co2
    END as co2_outlet
    CASE 
        WHEN sensor_id = 3 THEN avg_co2
    END as co2_inlet
    CASE 
        WHEN sensor_id = 2 THEN avg_temperature
    END as temp_outlet
    CASE 
        WHEN sensor_id = 2 THEN avg_temperature
    END as temp_inlet
    CASE 
        WHEN sensor_id = 51 THEN avg_temperature
    END as temp_tk
    CASE 
        WHEN sensor_id = 2 THEN avg_temperature
    END as humid_outlet
    CASE 
        WHEN sensor_id = 3 THEN avg_temperature
    END as hunmid_inlet
  FROM (
    SELECT
            sensor_type,
            sensor_id,
            cyclicName,
            strftime('%Y-%m-%d %H:%M:00', datetime/1000, 'unixepoch', '+7 hours') AS minute_th,
            1000 * (
                (CAST(strftime('%s', datetime/1000, 'unixepoch', '+7 hours') AS INTEGER) / 60) * 60
            ) AS minute_th_ms,

            AVG(co2) AS avg_co2,
            AVG(temperature) AS avg_temperature,
            AVG(humidity) AS avg_humidity,

            AVG(
                CASE
                WHEN sensor_id = 2  THEN (1.023672650 * co2) - 19.479471
                WHEN sensor_id = 3  THEN (0.970384222 * co2) - 99.184335
                WHEN sensor_id = 51 THEN 0
                END
            ) AS avg_co2_adjust,

            MIN(
                CASE
                WHEN sensor_id = 2  THEN 'Co2_Outlet'
                WHEN sensor_id = 3  THEN 'Co2_Inlet'
                WHEN sensor_id = 51 THEN 'TK'
                END
            ) AS sensor_name,

            COUNT(*) AS samples
            FROM hlr_sensor_data
            WHERE datetime BETWEEN ? AND ?
            GROUP BY sensor_type, sensor_id, minute_th, cyclicName
) 
GROUP BY minute_th, cyclicName