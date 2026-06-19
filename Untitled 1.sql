// What will the weather be like in Boston next weekend?
/*
Our food truck is scheduled to be at a sporting event in Boston next weekend. Can you tell me the forecasted weather so we can anticipate footfall traffic and prepare food accordingly?
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    avg_temperature_air_2m_f,
    avg_humidity_relative_2m_pct,
    avg_wind_speed_10m_mph,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_cloud_cover_tot_pct,
    probability_of_precipitation_pct,
    probability_of_snow_pct
FROM
(
    SELECT
        postal_code,
        country,
        date_valid_std,
        avg_temperature_air_2m_f,
        avg_humidity_relative_2m_pct,
        avg_wind_speed_10m_mph,
        tot_precipitation_in,
        tot_snowfall_in,
        avg_cloud_cover_tot_pct,
        probability_of_precipitation_pct,
        probability_of_snow_pct,
        DATEADD(DAY,2,CURRENT_DATE()) AS skip_date,
        DATEADD(DAY,7 - DAYOFWEEKISO(skip_date),skip_date) AS next_sunday,
        DATEADD(DAY,-1,next_sunday) AS next_saturday
    FROM
        onpoint_id.forecast_day
    WHERE
        postal_code = '02201' AND
        country = 'US'
)
WHERE
    date_valid_std IN (next_saturday,next_sunday)
ORDER BY
    date_valid_std
;

// We are looking  for new locations for our food trucks in Paris.
/*
We would like to find new locations for our food trucks in Paris. Can we look at last year’s weather in Paris to choose the best location?
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    avg_temperature_air_2m_f,
    avg_humidity_relative_2m_pct,
    avg_wind_speed_10m_mph,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_cloud_cover_tot_pct
FROM
    onpoint_id.history_day
WHERE
    postal_code = '75008' AND
    country = 'FR' AND
    date_valid_std = DATEADD(year,-1,CURRENT_DATE)
;

// What is the temperature in London during May?
/*
We would like to look at the temperatures in May of last year to determine when to rotate seasonal menu items.
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    min_temperature_air_2m_f,
    avg_temperature_air_2m_f,
    max_temperature_air_2m_f
FROM
    onpoint_id.history_day
WHERE
    postal_code = 'SW1A 0AA' AND
    country = 'GB' AND
    date_valid_std BETWEEN DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,5,1) AND DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,5,31)
ORDER BY
    date_valid_std
;

// What will the temperature in Tokyo be next Saturday?
/*
Our food truck is catering an outdoor party in Tokyo next Saturday? Can you tell me the forecasted temperatures so we can determine our menu items?
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    min_temperature_air_2m_f,
    avg_temperature_air_2m_f,
    max_temperature_air_2m_f
FROM
(
    SELECT
        postal_code,
        country,
        date_valid_std,
        min_temperature_air_2m_f,
        avg_temperature_air_2m_f,
        max_temperature_air_2m_f,
        DATEADD(DAY,2,CURRENT_DATE()) AS skip_date,
        DATEADD(DAY,6 - DAYOFWEEKISO(skip_date),skip_date) AS next_saturday
    FROM
        onpoint_id.forecast_day
    WHERE
        postal_code = '102-0082' AND
        country = 'JP'
)
WHERE
    date_valid_std = next_saturday
;

// I would like to look at last year's weather in Sydney from September - November.
/*
We would like to look at the weather last year from September to November in Sydney so we can determine the best location to park our food truck during those months.
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    avg_temperature_air_2m_f,
    avg_humidity_relative_2m_pct,
    avg_wind_speed_10m_mph,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_cloud_cover_tot_pct
FROM
    onpoint_id.history_day
WHERE
    postal_code = '2000' AND
    country = 'AU' AND
    date_valid_std BETWEEN DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,9,1) AND DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,11,30)
ORDER BY
    date_valid_std
;

