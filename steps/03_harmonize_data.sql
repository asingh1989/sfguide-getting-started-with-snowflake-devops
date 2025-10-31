-- Set environment variable (default to 'dev' if not provided)
SET environment = COALESCE($environment, 'dev');

-- Set database context from parameter
SET DATABASE_NAME = COALESCE($database_name, CONCAT('QUICKSTART_', $environment));
USE DATABASE IDENTIFIER($DATABASE_NAME);
USE SCHEMA silver;
-- Views to transform marketplace data in pipeline
-- This file contains only SQL and can be executed with EXECUTE IMMEDIATE FROM

-- Flight emissions view
CREATE OR REPLACE VIEW flight_emissions AS
SELECT 
    departure_airport, 
    arrival_airport, 
    AVG(estimated_co2_total_tonnes / seats) * 1000 AS co2_emissions_kg_per_person
FROM oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
WHERE seats != 0 AND estimated_co2_total_tonnes IS NOT NULL
GROUP BY departure_airport, arrival_airport;

-- Flight punctuality view
CREATE OR REPLACE VIEW flight_punctuality AS
SELECT 
    departure_iata_airport_code, 
    arrival_iata_airport_code, 
    COUNT(
        CASE WHEN arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END
    ) / COUNT(*) * 100 AS punctual_pct
FROM oag_flight_status_data_sample.public.flight_status_latest_sample
WHERE arrival_actual_ingate_timeliness IS NOT NULL
GROUP BY departure_iata_airport_code, arrival_iata_airport_code;

-- Flights from home view
CREATE OR REPLACE VIEW flights_from_home AS
SELECT 
    fe.departure_airport, 
    fe.arrival_airport, 
    a.city_name AS arrival_city,  
    fe.co2_emissions_kg_per_person, 
    fp.punctual_pct
FROM flight_emissions fe
JOIN flight_punctuality fp
    ON fe.departure_airport = fp.departure_iata_airport_code 
    AND fe.arrival_airport = fp.arrival_iata_airport_code
LEFT JOIN AIRPORTS a
    ON fe.arrival_airport = a.iata_code
WHERE fe.departure_airport = (
    SELECT $1:airport 
    FROM @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
        (FILE_FORMAT => bronze.json_format));

-- Weather forecast view
CREATE OR REPLACE VIEW weather_forecast AS
SELECT 
    postal_code, 
    AVG(avg_temperature_air_2m_f) avg_temperature_air_f, 
    AVG(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, 
    AVG(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, 
    AVG(probability_of_precipitation_pct) precipitation_probability_pct
FROM global_weather__climate_data_for_bi.standard_tile.forecast_day
WHERE country = 'US'
GROUP BY postal_code;

-- Major US cities view
CREATE OR REPLACE VIEW major_us_cities AS
SELECT 
    geo.geo_id, 
    geo.geo_name, 
    MAX(ts.value) total_population
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.DATACOMMONS_TIMESERIES ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX geo 
    ON ts.geo_id = geo.geo_id
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS geo_rel 
    ON geo_rel.related_geo_id = geo.geo_id
WHERE TRUE
    AND ts.variable_name = 'Total Population, census.gov'
    AND date >= '2020-01-01'
    AND geo.level = 'City'
    AND geo_rel.geo_id = 'country/USA'
    AND value > 100000
GROUP BY geo.geo_id, geo.geo_name
ORDER BY total_population DESC;

-- Zip codes in city view
CREATE OR REPLACE VIEW zip_codes_in_city AS
SELECT 
    city.geo_id city_geo_id, 
    city.geo_name city_geo_name, 
    city.related_geo_id zip_geo_id, 
    city.related_geo_name zip_geo_name
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS country
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS city 
    ON country.related_geo_id = city.geo_id
WHERE TRUE
    AND country.geo_id = 'country/USA'
    AND city.level = 'City'
    AND city.related_level = 'CensusZipCodeTabulationArea'
ORDER BY city_geo_id;

-- Weather joined with major cities view
CREATE OR REPLACE VIEW weather_joined_with_major_cities AS
SELECT 
    city.geo_id, 
    city.geo_name, 
    city.total_population,
    AVG(avg_temperature_air_f) avg_temperature_air_f,
    AVG(avg_relative_humidity_pct) avg_relative_humidity_pct,
    AVG(avg_cloud_cover_pct) avg_cloud_cover_pct,
    AVG(precipitation_probability_pct) precipitation_probability_pct
FROM major_us_cities city
JOIN zip_codes_in_city zip ON city.geo_id = zip.city_geo_id
JOIN weather_forecast weather ON zip.zip_geo_name = weather.postal_code
GROUP BY city.geo_id, city.geo_name, city.total_population;

-- Attractions view
CREATE OR REPLACE VIEW attractions AS
SELECT
    city.geo_id,
    city.geo_name,
    COUNT(CASE WHEN category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
    COUNT(CASE WHEN category_main = 'Zoo' THEN 1 END) zoo_cnt,
    COUNT(CASE WHEN category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_INDEX poi
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS poi_add 
    ON poi_add.poi_id = poi.poi_id
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.US_ADDRESSES address 
    ON address.address_id = poi_add.address_id
JOIN major_us_cities city ON city.geo_id = address.id_city
WHERE TRUE
    AND category_main IN ('Aquarium', 'Zoo', 'Korean Restaurant')
    AND id_country = 'country/USA'
GROUP BY city.geo_id, city.geo_name;