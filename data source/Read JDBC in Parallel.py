# 1. Read JDBC in Parallel
# I will use the PySpark jdbc() method and option numPartitions to read this table in parallel into DataFrame. This property also determines the maximum number of concurrent JDBC connections to use. The below example creates the DataFrame with 5 partitions.


# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
           .getOrCreate()

# Query table using jdbc()
df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/emp", "employee", \
          properties={"user": "root", "password": "root", "driver":"com.mysql.cj.jdbc.Driver"})

# show DataFrame
df.show()


# pyspark parallel read jdbc
# 2. Another Approach to Read JDBC in Parallel
# Alternatively, you can also use the DataFrameReader.format("jdbc").load() to read the table. When you use this, you need to provide the database details with option() method.


# Read Table in Parallel
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("dbtable","employee") \
    .option("numPartitions",5) \
    .option("user", "root") \
    .option("password", "root") \
    .load()
# You can also select the specific columns with where condition by using the pyspark jdbc query option to read in parallel. Note that you can use either dbtable or query option but not both at a time. Also, when using the query option, you can’t use partitionColumn option.


# Select columns with where clause
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("query","select id,age from employee where gender='M'") \
    .option("numPartitions",5) \
    .option("user", "root") \
    .option("password", "root") \
    .load()
# 3. Using fetchsize with numPartitions to Read
# The fetchsize is another option which is used to specify how many rows to fetch at a time, by default it is set to 10. The JDBC fetch size determines how many rows to retrieve per round trip which helps the performance of JDBC drivers.  Do not set this to very large number as you might see issues.


# Using fetchsize
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("query","select id,age from employee where gender='M'") \
    .option("numPartitions",5) \
    .option("fetchsize", 20) \
    .option("user", "root") \
    .option("password", "root") \
    .load()
# 4. Other JDBC Options to read in Parallel
# Note that when one option from the below table is specified you need to specify all of them along with numPartitions.
# 
# They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric, date, or timestamp column from the table in question
# 
# JDBC OPTION	DESCRIPTION
# partitionColumn	Partition column to specify
# lowerBound	Lower Bound
# upperBound	Upper Bound