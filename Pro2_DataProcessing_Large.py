#!/usr/bin/env python
# utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from faker import Factory

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV Anonymization") \
    .getOrCreate()

# Read large CSV file
df = spark.read.csv('demyst_input_dataset.csv', header=True, inferSchema=True)

# Create UDF to mask sensitive data using Faker 
def anonymize_name():
    faker = Factory.create()
    return faker.name()

anonymize_name_udf = udf(anonymize_name, StringType())

# Apply Faker to anonymize or mask sensitive data

df_anonymize = df.withColumn('first_name', anonymize_name_udf(col('first_name'))) \
                  .withColumn('last_name', anonymize_name_udf(col('last_name'))) \
                  .withColumn('address', anonymize_name_udf(col('address')))

# Write anonymized data back to CSV
df_anonymize.write.csv('demyst_output_large_dataset_spark.csv', header=True)
