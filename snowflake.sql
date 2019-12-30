//Use the INTERVIEW_WH warehouse
USE WAREHOUSE INTERVIEW_WH;

//Use your USER_<name> database
USE DATABASE USER_JULIE_FLATER;



//Create tables based on the defined datasets above

create schema northwoods;


create or replace table airlines (
	IATA_CODE VARCHAR comment 'Airline identifier',
	AIRLINE VARCHAR comment 'Airline name'
) 
comment='Airline codes and names';


create or replace table airports (
	IATA_CODE VARCHAR comment 'Airline identifier',
	AIRPORT VARCHAR comment 'Airport name',
	CITY VARCHAR comment 'City where the airport is located',
	STATE VARCHAR comment 'State where the airport is located',
	COUNTRY VARCHAR comment 'Country where the airport is located',
	LATITUDE NUMBER comment 'Latitude of the airport',
	LONGITUDE NUMBER comment 'Longitude of the airport'
) 
comment='Airport codes, names, and locations';


create or replace table flights (
	YEAR NUMBER comment 'Year of the flight',
	MONTH NUMBER comment 'Month of the flight',
	DAY NUMBER comment 'Day of the flight',
	DAY_OF_WEEK NUMBER comment 'Day of the week of the flight',
	AIRLINE VARCHAR comment 'Airline identifier',
	FLIGHT_NUMBER VARCHAR comment 'Flight identifier',
	TAIL_NUMBER VARCHAR comment 'Aircraft identifier',
	ORIGIN_AIRPORT VARCHAR comment 'Starting airport',
	DESTINATION_AIRPORT VARCHAR comment 'Destination airport',
	SCHEDULED_DEPARTURE VARCHAR comment 'Planned departure time',
	DEPARTURE_TIME VARCHAR comment 'Wheels up time',
	DEPARTURE_DELAY NUMBER comment 'Total delay on departure',
	TAXI_OUT NUMBER comment 'Time duration between departure from origin gate to wheels off',
	WHEELS_OFF VARCHAR comment 'Time point the aircraft\'s wheels leave the ground',
	SCHEDULED_TIME NUMBER comment 'Planned time amount needed for the flight trip',
	ELAPSED_TIME NUMBER comment 'Total trip time',
	AIR_TIME NUMBER comment 'Time duration between wheels_off and wheels_on time',
	DISTANCE NUMBER comment 'The distance between two airports',
	WHEELS_ON NUMBER comment 'The time point that the aircraft\'s wheels touch on the ground',
	TAXI_IN NUMBER comment 'Time duration between wheels-on and gate arrival at destination airport',
	SCHEDULED_ARRIVAL NUMBER comment 'Planned arrival time',
	ARRIVAL_TIME VARCHAR comment 'Time of arrival',
	ARRIVAL_DELAY VARCHAR comment 'arrival_time - scheduled_arrival',
	DIVERTED NUMBER comment 'Aircraft was diverted',
	CANCELLED NUMBER comment 'Aircraft was canceled',
	CANCELLATION_REASON VARCHAR comment 'Reason for Cancellation of flight: A - Airline/Carrier; B - Weather; C - National Air System; D - Security',
	AIR_SYSTEM_DELAY NUMBER comment 'Delay caused by the air system',
	SECURITY_DELAY NUMBER comment 'Delay caused by security',
	AIRLINE_DELAY NUMBER comment 'Delay caused by the airline',
	LATE_AIRCRAFT_DELAY NUMBER comment 'Delay caused by the aircraft',
	WEATHER_DELAY NUMBER comment 'Delay caused by weather'
) 
comment='Flight records';



//Load data from the external stage into corresponding tables

copy into airlines 
from 's3://nml-interview-datasets/flight-data/csv/airlines.csv' 
CREDENTIALS = (aws_key_id='AKIAZIV2MVLEWC62V3PU' aws_secret_key='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')
FILE_FORMAT = (type = csv, skip_header = 1);



copy into airports 
from 's3://nml-interview-datasets/flight-data/csv/airports.csv' 
CREDENTIALS = (aws_key_id='AKIAZIV2MVLEWC62V3PU' aws_secret_key='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')
FILE_FORMAT = (type = csv, skip_header = 1);
//VALIDATION_MODE = RETURN_ERRORS;



copy into flights 
from 's3://nml-interview-datasets/flight-data/csv/flights.csv' 
CREDENTIALS = (aws_key_id='AKIAZIV2MVLEWC62V3PU' aws_secret_key='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')
file_format = (type = csv, skip_header = 1);
//VALIDATION_MODE = RETURN_ERRORS;






//          REPORTS

// Total number of flights by airline and airport on a monthly basis 
//(assumptions: by origin airport)

CREATE OR REPLACE VIEW flights_by_airline_airport_month AS (
SELECT 
    f.airline AS airline_iata_code,
    al.airline, 
    f.origin_airport AS origin_airport_iata_code,
    IFF(ap.airport IS NULL, 'Unknown', ap.airport) AS origin_airport, 
    f.month,
    COUNT(*) AS number_of_flights
FROM "USER_JULIE_FLATER"."NORTHWOODS"."FLIGHTS" f
LEFT OUTER JOIN "USER_JULIE_FLATER"."NORTHWOODS"."AIRLINES" al
    ON f.airline = al.iata_code
LEFT OUTER JOIN "USER_JULIE_FLATER"."NORTHWOODS"."AIRPORTS" ap
    ON f.origin_airport = ap.iata_code
GROUP BY 1, 2, 3, 4, 5
ORDER BY 2 ASC, 5 ASC, 4 ASC);





//On time percentage of each airline for the year 2015
//assumptions: "on time" means arrival BEFORE or AT scheduled arrival; only considering flights where arrival_delay data is available

CREATE OR REPLACE VIEW on_time_pct_by_airline AS (
WITH on_time_count_by_airline AS (
  SELECT airline, COUNT(*) AS count_on_time
  FROM flights
  WHERE arrival_delay <= 0 
  AND year = 2015
  GROUP BY 1
),

total_flights_with_known_delay_by_airline AS (
  SELECT airline, COUNT(*) AS total_flights
  FROM flights
  WHERE arrival_delay != 'NULL'
  AND arrival_delay IS NOT NULL
  AND year = 2015
  GROUP BY 1
)

SELECT 
    total.airline AS airline_iata_code,
    airlines.airline, 
    (TO_NUMBER(on_time.count_on_time/total.total_flights * 100, 4, 2)) AS percent_on_time
FROM total_flights_with_known_delay_by_airline total
LEFT OUTER JOIN on_time_count_by_airline on_time
    ON total.airline = on_time.airline
LEFT OUTER JOIN airlines 
    ON total.airline = airlines.iata_code AND on_time.airline = airlines.iata_code
ORDER BY 3 DESC);




// Airlines with the largest number of delays
// assumption: value in <type>_delay fields represent time in minutes, not counts
CREATE OR REPLACE VIEW delays_by_airline AS (
WITH delays_by_flight AS (
SELECT 
    airline, 
    IFF (air_system_delay > 0, 1, 0) AS asd,
    IFF (security_delay > 0, 1, 0) AS sd,
    IFF (airline_delay > 0, 1, 0) AS ad,
    IFF (late_aircraft_delay > 0, 1, 0) AS lad,
    IFF (weather_delay > 0, 1, 0) AS wd
FROM flights)

SELECT
    d.airline AS airline_iata_code,
    a.airline,
    SUM(d.asd) AS total_air_system_delays,
    SUM(d.sd) AS total_security_delays,
    SUM(d.ad) AS total_airline_delays,
    SUM(d.lad) AS total_late_aircraft_delays,
    SUM(d.wd) AS total_weather_delays,
    (total_air_system_delays + total_security_delays + total_airline_delays + total_late_aircraft_delays + total_weather_delays) AS total_cumulative_delays
FROM delays_by_flight d
LEFT OUTER JOIN airlines a 
    ON d.airline = a.iata_code
GROUP BY 1, 2
ORDER BY 8 DESC);



//Cancellation reasons by airport
//Reason for Cancellation of flight: A - Airline/Carrier; B - Weather; C - National Air System; D - Security
// assumptions: using origin_airport, this report is requesting COUNTS of each reason grouped by airport. Some airports with cancellation data are not in airlines table, they're listed as 'Unknown'

CREATE OR REPLACE VIEW cancellation_reasons_by_airport AS (
WITH cancellations AS (
SELECT 
    origin_airport,  
    IFF (cancellation_reason = 'A', 1, 0) AS cancellation_a,
    IFF (cancellation_reason = 'B', 1, 0) AS cancellation_b,
    IFF (cancellation_reason = 'C', 1, 0) AS cancellation_c,
    IFF (cancellation_reason = 'D', 1, 0) AS cancellation_d
FROM flights
WHERE cancellation_reason IS NOT NULL
)

SELECT
    c.origin_airport AS airport_iata_code,
    IFF(a.airport IS NULL, 'Unknown', a.airport) AS airport,
    SUM(c.cancellation_a) AS total_airline_cancellations,
    SUM(c.cancellation_b) AS total_weather_cancellations,
    SUM(c.cancellation_c) AS total_national_air_system_cancellations,
    SUM(c.cancellation_d) AS total_security_cancellations,
    (total_airline_cancellations + total_weather_cancellations + total_national_air_system_cancellations + total_security_cancellations) AS total_cumulative_cancellations
FROM cancellations c
LEFT OUTER JOIN airports a 
    ON c.origin_airport = a.iata_code
GROUP BY 1, 2
ORDER BY 7 DESC);





// Delay reasons by airport

CREATE OR REPLACE VIEW delays_by_airport AS (
WITH delays AS (
SELECT 
    origin_airport, 
    IFF (air_system_delay > 0, 1, 0) AS asd,
    IFF (security_delay > 0, 1, 0) AS sd,
    IFF (airline_delay > 0, 1, 0) AS ad,
    IFF (late_aircraft_delay > 0, 1, 0) AS lad,
    IFF (weather_delay > 0, 1, 0) AS wd
FROM flights)

SELECT
    d.origin_airport AS airport_iata_code,
    IFF(a.airport IS NULL, 'Unknown', a.airport) AS airport,
    SUM(d.asd) AS total_air_system_delays,
    SUM(d.sd) AS total_security_delays,
    SUM(d.ad) AS total_airline_delays,
    SUM(d.lad) AS total_late_aircraft_delays,
    SUM(d.wd) AS total_weather_delays,
    (total_air_system_delays + total_security_delays + total_airline_delays + total_late_aircraft_delays + total_weather_delays) AS total_cumulative_delays
FROM delays d
LEFT OUTER JOIN airports a 
    ON d.origin_airport = a.iata_code
GROUP BY 1, 2
ORDER BY 8 DESC);




// Airline with the most unique routes


CREATE OR REPLACE VIEW unique_routes_by_airline AS (
SELECT
    f.airline AS airline_iata_code,
    a.airline,
    COUNT(DISTINCT f.origin_airport, f.destination_airport) AS unique_routes
FROM flights f
LEFT OUTER JOIN airlines a
    ON f.airline = a.iata_code
GROUP BY 1,2
ORDER BY 3 DESC);

