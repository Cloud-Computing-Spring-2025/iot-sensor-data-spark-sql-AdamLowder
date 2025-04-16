from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, hour, avg, to_timestamp, dense_rank, round as spark_round
from pyspark.sql.window import Window

#initialize spark session
spark = SparkSession.builder.appName("IoT Sensor Data Analytics").getOrCreate()

#define schema and load data
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("location", StringType(), True),
    StructField("sensor_type", StringType(), True),
])
df = spark.read.csv("sensor_data.csv", header=True, schema=schema)

#task 1:load and basic exploration

#show first 5 rows
df.show(5)
#total number of records
print("Total Records:", df.count())
#distinct locations
df.select("location").distinct().show()
#create temporary view
df.createOrReplaceTempView("sensor_readings")
#save output
df.write.csv("task1_output.csv", header=True, mode="overwrite")


#task 2:filtering and simple aggregations

#filter temperature out of range
in_range = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
out_of_range = df.filter((col("temperature") < 18) | (col("temperature") > 30))
print("In-Range Count:", in_range.count())
print("Out-of-Range Count:", out_of_range.count())
#aggregation by location
agg_df = df.groupBy("location").agg(spark_round(avg("temperature"), 2).alias("avg_temperature"),spark_round(avg("humidity"), 2).alias("avg_humidity")).orderBy("avg_temperature", ascending=False)
agg_df.show()
agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")


#task 3:time based analysis
#convert timestamp column to proper timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("hour_of_day", hour("timestamp"))
#update temp view
df.createOrReplaceTempView("sensor_readings")
#group by hour and find average temperature
hourly_avg = df.groupBy("hour_of_day").agg(spark_round(avg("temperature"), 2).alias("avg_temp")).orderBy("hour_of_day")
hourly_avg.show()
hourly_avg.write.csv("task3_output.csv", header=True, mode="overwrite")


#task 4:window function - rank sensors
sensor_avg_temp = df.groupBy("sensor_id").agg(spark_round(avg("temperature"), 2).alias("avg_temp"))
window_spec = Window.orderBy(col("avg_temp").desc())
ranked_sensors = sensor_avg_temp.withColumn("rank_temp", dense_rank().over(window_spec))
top5 = ranked_sensors.limit(5)
top5.show()
#save output
top5.write.csv("task4_output.csv", header=True, mode="overwrite")

#task 5:pivot and interpretation
# Create pivot table by location and hour_of_day
pivot_table = df.groupBy("location").pivot("hour_of_day").agg(spark_round(avg("temperature"), 2)).orderBy("location")
pivot_table.show()
#save output
pivot_table.write.csv("task5_output.csv", header=True, mode="overwrite")