# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget 
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```
3.3.2
```

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 25MB

```

from pyspark.sql import SparkSession
import os
import requests

# Create a Spark session
spark = SparkSession.builder.appName("NYC_Taxi").getOrCreate()

# Define file path
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet"
file_path = "/content/yellow_tripdata_2024-10.parquet"

# Download the dataset if it doesn't exist
if not os.path.exists(file_path):
    response = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(response.content)

# Read the Parquet file into a Spark DataFrame
df = spark.read.parquet(file_path)

# Repartition the DataFrame into 4 partitions
df_repartitioned = df.repartition(4)

# Save the repartitioned DataFrame to Parquet
output_dir = "/content/output_parquet"
df_repartitioned.write.mode("overwrite").parquet(output_dir)

# Calculate average size of the Parquet files
parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in parquet_files)
average_size_mb = total_size / len(parquet_files) / (1024 * 1024)  # Convert bytes to MB

# Print the average file size
print(f"Average Parquet file size: {average_size_mb:.2f} MB")


```



## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 125,567

```

from pyspark.sql.functions import col, to_date

# Convert pickup timestamp to date format and filter trips from October 15th
df_filtered = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))).filter(col("pickup_date") == "2024-10-15")

# Count the number of trips on October 15th
trip_count = df_filtered.count()

# Print the result
print(f"Number of taxi trips on October 15th, 2024: {trip_count}")

```


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 162

```

from pyspark.sql.functions import col, unix_timestamp

# Compute trip duration in hours
df_duration = df.withColumn("trip_duration_hours", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600)

# Find the maximum trip duration
longest_trip_hours = df_duration.selectExpr("MAX(trip_duration_hours) AS max_duration").collect()[0]["max_duration"]

# Print the result
print(f"Longest trip duration in hours: {longest_trip_hours:.2f} hours")

```



## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 4040



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island

```
from pyspark.sql.functions import col, count
# Download and load the Zone Lookup data
!wget -q https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# Read Zone Lookup data into a Spark DataFrame
zone_df = spark.read.csv("/content/taxi_zone_lookup.csv", header=True, inferSchema=True)

# Load the Yellow Taxi October 2024 dataset
file_path = "/content/yellow_tripdata_2024-10.parquet"
df = spark.read.parquet(file_path)

# Register both DataFrames as temporary views
df.createOrReplaceTempView("yellow_taxi")
zone_df.createOrReplaceTempView("zone_lookup")

# SQL query to find the least frequent pickup location
query = """
SELECT z.Zone, COUNT(y.PULocationID) AS pickup_count
FROM yellow_taxi y
JOIN zone_lookup z ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickup_count ASC
LIMIT 1
"""

# Execute the query
least_frequent_zone = spark.sql(query)

# Show the result
least_frequent_zone.show()

```


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
