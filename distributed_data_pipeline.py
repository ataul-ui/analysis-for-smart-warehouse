from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat
from pyspark.sql import functions as F



#OR JUST CREATE A DATA PIPELINE HERE,
# THIS COULD BE ANOTHER DATAOPS PROJECT I THINK
# USE SPARK STREAMING TO CREATE A NEW DATASET AND STUFF

#will use random forrest here I think 
# to give a binary output on which sensors to remove
# based on activation amount

# mlflow will be used here, MLProject will be saved
# to a folder called models (like the data folder)
# this script will be triggered by a github actions workflow on cron job
# so that as new data gets uploaded to data folder new models can be created
# I will provide reference to dvc in blog post for this (point them to alzheimers project)


# maybe create model that tell the optimal route for a worker to take
# to avoid the robot

# or create a model to do something else, idk I'll figure something out,
# I just need to use spark sql, spark ml and spark streaming here

# all this is in prep for my certification, yeah this is good













# from this we can see some sensors can be removed, but the 
#amount of data is massive, lets use spark to count which sensors don't get activated

# Create a SparkSession
spark = SparkSession.builder.appName("SensorAnalysis").getOrCreate()

# Load the dataset into a DataFrame
data_path = "data/tracklets_0114.txt"  # Replace with the local path to the downloaded dataset
dl = spark.read.option("header", "true").option("inferSchema", "true").option("sep","\t").csv(data_path)
#dl.show(10)
#dl.select("begin_time").show(10)
#dl.select("begin_sensor").show(10)


# Convert 'begin_time' column to datetime format
dl = dl.withColumn("begin_time", F.from_unixtime(dl["begin_time"] / 1000).cast("timestamp"))
# Convert 'end_time' column to datetime format
dl = dl.withColumn("end_time", F.from_unixtime(dl["end_time"] / 1000).cast("timestamp"))

# Filter the DataFrame to include only rows with sensor_id between 250 and 450
filtered_df = dl.filter((col("begin_sensor") >= 250) & (col("begin_sensor") <= 450))

# Concatenate "begin_sensor" and "end_sensor" into a single column to count activations
combined_sensors = filtered_df.select(concat(col("begin_sensor"), col("end_sensor")).alias("sensor"))

# Get the unique sensors within the specified range
all_sensors = spark.range(250, 451).withColumnRenamed("id", "sensor")

# Count the number of activations for each sensor
sensor_counts = combined_sensors.groupBy("sensor").count()

# Calculate the percentage of activations for each sensor
total_activations = combined_sensors.count()
sensor_counts = sensor_counts.withColumn("activation_percentage", col("count") / total_activations * 100)

# Identify sensors with low activation percentages (e.g., less than 1%)
low_activation_sensors = sensor_counts.filter(col("activation_percentage") > 10.0)

# Display the sensors that can potentially be dropped
low_activation_sensors.show(200)

spark.stop()

'''# Perform a left outer join to include sensors with 0 activations
all_sensors_with_counts = all_sensors.join(sensor_counts, on="sensor", how="left_outer")

# Fill null values with 0 for sensors with 0 activations
all_sensors_with_counts = all_sensors_with_counts.fillna(0, subset=["count", "activation_percentage"])

# Identify sensors with low activation percentages (e.g., less than 1%)
low_activation_sensors = all_sensors_with_counts.filter(col("activation_percentage") < 10.0)

# Display the sensors that can potentially be dropped
low_activation_sensors.show(200)

# Stop the SparkSession
spark.stop()'''

'''
dl = dl.head(100000)
figure(figsize=(8, 6), dpi=80)
plt.scatter(dl['begin_time'], dl['begin_sensor'], s=0.005)

plt.show() '''