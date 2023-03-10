
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder
           .appName('SparkByExamples.com')
           .config("spark.jars", "mysql-connector-java-8.0.13.jar")
           .getOrCreate()

# Create DataFrame 
columns = ["id", "name","age","gender"]
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Write to MySQL Table
sampleDF.write \
  .format("jdbc") \
  .option("driver","com.mysql.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/database_name") \
  .option("dbtable", "employee") \
  .option("user", "root") \
  .option("password", "root") \
  .save()

# Read from MySQL Table
val df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/database_name") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df.show()
