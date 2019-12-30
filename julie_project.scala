// Databricks notebook source
displayHTML("""<font size="6" color="red" face="sans-serif">Northwoods Airlines Competitive Analysis Dashboard</font>""")


// COMMAND ----------

// The application should read in the provided S3 CSV as data frames

val airlines_df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("s3a://AKIAZIV2MVLEWC62V3PU:etDt4lEnUAYNK095wn8XhWS8n4N06SWi98XkuYQj@nml-interview-datasets/flight-data/csv/airlines.csv")

val airports_df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("s3a://AKIAZIV2MVLEWC62V3PU:etDt4lEnUAYNK095wn8XhWS8n4N06SWi98XkuYQj@nml-interview-datasets/flight-data/csv/airports.csv")

val flights_df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("s3a://AKIAZIV2MVLEWC62V3PU:etDt4lEnUAYNK095wn8XhWS8n4N06SWi98XkuYQj@nml-interview-datasets/flight-data/csv/flights.csv")


// COMMAND ----------

// Total number of flights by airline and airport on a monthly basis
import org.apache.spark.sql.functions._


val flightsByAirlineAirport = 
flights_df
  .join(airlines_df, flights_df("AIRLINE") === airlines_df("IATA_CODE")).drop(flights_df("AIRLINE"))
  .select("IATA_CODE", "AIRLINE", "ORIGIN_AIRPORT", "MONTH")
  .groupBy(airlines_df("IATA_CODE"), airlines_df("AIRLINE"), flights_df("ORIGIN_AIRPORT"), flights_df("MONTH"))
  .agg(count(flights_df.col("*")).alias("NUM_OF_FLIGHTS"))
  .withColumnRenamed("IATA_CODE", "AIRLINE_CODE")
  .join(airports_df, flights_df("ORIGIN_AIRPORT") === airports_df("IATA_CODE"))
  .withColumnRenamed("IATA_CODE", "AIRPORT_CODE")
  .select("AIRLINE_CODE", "AIRLINE", "AIRPORT_CODE", "AIRPORT", "MONTH", "NUM_OF_FLIGHTS")


// The application should write the data frame as Delta files on the Databricks file system
 flightsByAirlineAirport.write.format("delta").mode("overwrite").save("/mnt/delta/jf/flights_airline_airport")



// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/flights_airline_airport").orderBy("AIRLINE", "MONTH", "AIRPORT"))

// COMMAND ----------

//On time percentage of each airline for the year 2015

import org.apache.spark.sql.functions._

val total_flights = 
flights_df
  .filter("ARRIVAL_DELAY is not null").filter("YEAR == 2015")
  .select("AIRLINE")
  .groupBy("AIRLINE")
  .agg(count(flights_df.col("*")).alias("NUM_OF_FLIGHTS"))


val on_time_flights = 
flights_df
  .filter("ARRIVAL_DELAY <= 0").filter("YEAR == 2015")
  .select("AIRLINE")
  .groupBy("AIRLINE")
  .agg(count(flights_df.col("*")).alias("NUM_OF_FLIGHTS_ON_TIME"))


val pct_on_time = 
total_flights
  .join(on_time_flights, Seq("AIRLINE"))
  .withColumn("PERCENT_ON_TIME", format_number(col("NUM_OF_FLIGHTS_ON_TIME")/ col("NUM_OF_FLIGHTS") * 100, 2))


val result = 
pct_on_time
  .join(airlines_df, pct_on_time("AIRLINE") === airlines_df("IATA_CODE")).drop(pct_on_time("AIRLINE"))
  .select("IATA_CODE", "AIRLINE", "PERCENT_ON_TIME")


// The application should write the data frame as Delta files on the Databricks file system
 result.write.format("delta").mode("overwrite").save("/mnt/delta/jf/on_time_pct")


// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/on_time_pct").orderBy(desc("PERCENT_ON_TIME")))

// COMMAND ----------

// Airlines with the largest number of delays
import org.apache.spark.sql.functions._

val delay_events =
flights_df
  .select("AIRLINE", "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
  .withColumn("ASD", when($"AIR_SYSTEM_DELAY" > 0, 1).otherwise(0))
  .withColumn("SD", when($"SECURITY_DELAY" > 0, 1).otherwise(0))
  .withColumn("AD", when($"AIRLINE_DELAY" > 0, 1).otherwise(0))
  .withColumn("LAD", when($"LATE_AIRCRAFT_DELAY" > 0, 1).otherwise(0))
  .withColumn("WD", when($"WEATHER_DELAY" > 0, 1).otherwise(0))
  .drop("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")


val airline_delays =
delay_events
  .join(airlines_df, delay_events("AIRLINE") === airlines_df("IATA_CODE")).drop(delay_events("AIRLINE"))
  .select("IATA_CODE", "AIRLINE", "ASD", "SD", "AD", "LAD", "WD")
  .groupBy("IATA_CODE", "AIRLINE")
  .agg(sum("ASD").alias("AIR_SYSTEM_DELAYS"), 
       sum("SD").alias("SECURITY_DELAYS"),
       sum("AD").alias("AIRLINE_DELAYS"),
       sum("LAD").alias("LATE_AIRCRAFT_DELAYS"),
       sum("WD").alias("WEATHER_DELAYS")
       )
  .withColumn("TOTAL_DELAYS", col("AIR_SYSTEM_DELAYS") + col("SECURITY_DELAYS") +
              col("AIRLINE_DELAYS") + col("LATE_AIRCRAFT_DELAYS") + col("WEATHER_DELAYS"))


// The application should write the data frame as Delta files on the Databricks file system
 airline_delays.write.format("delta").mode("overwrite").save("/mnt/delta/jf/airline_delays")


// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/airline_delays").orderBy(desc("TOTAL_DELAYS")))

// COMMAND ----------

// Cancellation reasons by airport
import org.apache.spark.sql.functions._

val cancellation_events =
flights_df
  .select("ORIGIN_AIRPORT", "CANCELLATION_REASON")
  .withColumn("A", when($"CANCELLATION_REASON" === "A", 1).otherwise(0))
  .withColumn("B", when($"CANCELLATION_REASON" === "B", 1).otherwise(0))
  .withColumn("C", when($"CANCELLATION_REASON" === "C", 1).otherwise(0))
  .withColumn("D", when($"CANCELLATION_REASON" === "D", 1).otherwise(0))
  .drop("CANCELLATION_REASON")


val total_cancellations =
cancellation_events
  .join(airports_df, cancellation_events("ORIGIN_AIRPORT") === airports_df("IATA_CODE")).drop("ORIGIN_AIRPORT")
  .select("IATA_CODE", "AIRPORT", "A", "B", "C", "D")
  .groupBy("IATA_CODE", "AIRPORT")
  .agg(sum("A").alias("AIRLINE"), 
       sum("B").alias("WEATHER"),
       sum("C").alias("NATIONAL_AIR_SYSTEM"),
       sum("D").alias("SECURITY")
       )
  .withColumn("TOTAL", col("AIRLINE") + col("WEATHER") + col("NATIONAL_AIR_SYSTEM") + col("SECURITY"))
  

// The application should write the data frame as Delta files on the Databricks file system
 total_cancellations.write.format("delta").mode("overwrite").save("/mnt/delta/jf/cancellations_airport")


// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/cancellations_airport").orderBy(desc("TOTAL")))

// COMMAND ----------

//Delay reasons by airport
import org.apache.spark.sql.functions._

val delay_events =
flights_df
  .select("ORIGIN_AIRPORT", "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
  .withColumn("ASD", when($"AIR_SYSTEM_DELAY" > 0, 1).otherwise(0))
  .withColumn("SD", when($"SECURITY_DELAY" > 0, 1).otherwise(0))
  .withColumn("AD", when($"AIRLINE_DELAY" > 0, 1).otherwise(0))
  .withColumn("LAD", when($"LATE_AIRCRAFT_DELAY" > 0, 1).otherwise(0))
  .withColumn("WD", when($"WEATHER_DELAY" > 0, 1).otherwise(0))
  .drop("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")


val results =
delay_events
  .join(airports_df, delay_events("ORIGIN_AIRPORT") === airports_df("IATA_CODE")).drop(delay_events("ORIGIN_AIRPORT"))
  .select("IATA_CODE", "AIRPORT", "ASD", "SD", "AD", "LAD", "WD")
  .groupBy("IATA_CODE", "AIRPORT")
  .agg(sum("ASD").alias("AIR_SYSTEM_DELAYS"), 
       sum("SD").alias("SECURITY_DELAYS"),
       sum("AD").alias("AIRLINE_DELAYS"),
       sum("LAD").alias("LATE_AIRCRAFT_DELAYS"),
       sum("WD").alias("WEATHER_DELAYS")
       )
  .withColumn("TOTAL_DELAYS", col("AIR_SYSTEM_DELAYS") + col("SECURITY_DELAYS") +
              col("AIRLINE_DELAYS") + col("LATE_AIRCRAFT_DELAYS") + col("WEATHER_DELAYS"))

// The application should write the data frame as Delta files on the Databricks file system
 results.write.format("delta").mode("overwrite").save("/mnt/delta/jf/delays_airport")


// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/delays_airport").orderBy(desc("TOTAL_DELAYS")))

// COMMAND ----------

//Airline with the most unique routes

import org.apache.spark.sql.functions._

val distinct_routes = 
flights_df
  .join(airlines_df, flights_df("AIRLINE") === airlines_df("IATA_CODE")).drop(flights_df("AIRLINE"))
  .select("IATA_CODE", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
  .groupBy("IATA_CODE", "AIRLINE")
  .agg(countDistinct("ORIGIN_AIRPORT", "DESTINATION_AIRPORT").alias("DISTINCT_ROUTES"))


// The application should write the data frame as Delta files on the Databricks 
 distinct_routes.write.format("delta").mode("overwrite").save("/mnt/delta/jf/distinct_routes")



// COMMAND ----------

display(spark.read.format("delta").load("/mnt/delta/jf/distinct_routes").orderBy(desc("DISTINCT_ROUTES")))

// COMMAND ----------

// MAGIC %fs ls /mnt/delta/jf
