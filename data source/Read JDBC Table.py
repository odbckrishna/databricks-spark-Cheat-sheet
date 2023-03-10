
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
           .getOrCreate()

# Read table using jdbc()
df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/emp", "employee",
          properties={"user": "root", "password": "root", "driver":"com.mysql.cj.jdbc.Driver"})

# show DataFrame
df.show()

# Read from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()
