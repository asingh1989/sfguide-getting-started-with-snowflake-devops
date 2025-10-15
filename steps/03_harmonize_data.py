# Views to transform marketplace data in pipeline


import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType



"""
To join the flight and location focused tables 
we need to cross the gap between the airport and cities domains. 
For this we make use of a Snowpark Python UDF. 
What's really cool is that Snowpark allows us to define a vectorized UDF 
making the processing super efficient as we don’t have to invoke the 
function on each row individually!

To compute the mapping between airports and cities, 
we use SnowflakeFile to read a JSON list from the pyairports package. 
The SnowflakeFile class provides dynamic file access, to stream files of any size.
"""
# --- Update: Use Snowpark UDF registration instead of UserDefinedFunction ---

# Create a Snowpark session (update connection parameters as needed)
connection_parameters = {
    "account": "YMCUNCX-JA18147",
    "user": "asingh92",
    "password": "Singh008@mr@it",
    "role": "ACCOUNTADMIN",
    "warehouse": "QUICKSTART_WH",
    "database": "QUICKSTART_prod",
    "schema": "silver"
}
session = Session.builder.configs(connection_parameters).create()


"""
To mangle the data into a more usable form, 
we make use of views to not materialize the marketplace data 
and avoid the corresponding storage costs. 
"""
# List of view definitions as SQL strings
pipeline = [
    {
        "name": "flight_emissions",
        "query": """
        create or replace view flight_emissions as
        select 
            departure_airport, 
            arrival_airport, 
            avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
        from oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
        where seats != 0 and estimated_co2_total_tonnes is not null
        group by departure_airport, arrival_airport
        """
    },
    {
        "name": "flight_punctuality",
        "query": """
        create or replace view flight_punctuality as
        select 
            departure_iata_airport_code, 
            arrival_iata_airport_code, 
            count(
                case when arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END
            ) / COUNT(*) * 100 as punctual_pct
        from oag_flight_status_data_sample.public.flight_status_latest_sample
        where arrival_actual_ingate_timeliness is not null
        group by departure_iata_airport_code, arrival_iata_airport_code
        """
    },
    {
        "name": "flights_from_home",
        "query": """
        create or replace view flights_from_home as
        select 
            fe.departure_airport, 
            fe.arrival_airport, 
            a.city_name as arrival_city,  
            fe.co2_emissions_kg_per_person, 
            fp.punctual_pct
        from flight_emissions fe
        join flight_punctuality fp
            on fe.departure_airport = fp.departure_iata_airport_code 
            and fe.arrival_airport = fp.arrival_iata_airport_code
        left join AIRPORTS a
            on fe.arrival_airport = a.iata_code
        where fe.departure_airport = (
            select $1:airport 
            from @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
                (FILE_FORMAT => bronze.json_format))
        """
    },
    {
        "name": "weather_forecast",
        "query": """
        create or replace view weather_forecast as
        select 
            postal_code, 
            avg(avg_temperature_air_2m_f) avg_temperature_air_f, 
            avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, 
            avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, 
            avg(probability_of_precipitation_pct) precipitation_probability_pct
        from global_weather__climate_data_for_bi.standard_tile.forecast_day
        where country = 'US'
        group by postal_code
        """
    },
    {
        "name": "major_us_cities",
        "query": """
        create or replace view major_us_cities as
        select 
            geo.geo_id, 
            geo.geo_name, 
            max(ts.value) total_population
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.DATACOMMONS_TIMESERIES ts
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX geo 
            on ts.geo_id = geo.geo_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS geo_rel 
            on geo_rel.related_geo_id = geo.geo_id
        where true
            and ts.variable_name = 'Total Population, census.gov'
            and date >= '2020-01-01'
            and geo.level = 'City'
            and geo_rel.geo_id = 'country/USA'
            and value > 100000
        group by geo.geo_id, geo.geo_name
        order by total_population desc
        """
    },
    {
        "name": "zip_codes_in_city",
        "query": """
        create or replace view zip_codes_in_city as
        select 
            city.geo_id city_geo_id, 
            city.geo_name city_geo_name, 
            city.related_geo_id zip_geo_id, 
            city.related_geo_name zip_geo_name
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS country
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS city 
            on country.related_geo_id = city.geo_id
        where true
            and country.geo_id = 'country/USA'
            and city.level = 'City'
            and city.related_level = 'CensusZipCodeTabulationArea'
        order by city_geo_id
        """
    },
    {
        "name": "weather_joined_with_major_cities",
        "query": """
        create or replace view weather_joined_with_major_cities as
        select 
            city.geo_id, 
            city.geo_name, 
            city.total_population,
            avg(avg_temperature_air_f) avg_temperature_air_f,
            avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
            avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
            avg(precipitation_probability_pct) precipitation_probability_pct
        from major_us_cities city
        join zip_codes_in_city zip on city.geo_id = zip.city_geo_id
        join weather_forecast weather on zip.zip_geo_name = weather.postal_code
        group by city.geo_id, city.geo_name, city.total_population
        """
    }
    {
        
        "name": "attractions",
         "query"="""
        select
        city.geo_id,
        city.geo_name,
        count(case when category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
        count(case when category_main = 'Zoo' THEN 1 END) zoo_cnt,
        count(case when category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt,
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_INDEX poi
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS poi_add 
        on poi_add.poi_id = poi.poi_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.US_ADDRESSES address 
        on address.address_id = poi_add.address_id
        join major_us_cities city on city.geo_id = address.id_city
        where true
        and category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
        and id_country = 'country/USA'
        group by city.geo_id, city.geo_name
        """
    }
    
]

# Create views in Snowflake using the session
for view in pipeline:
    session.sql(view["query"]).collect()