
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder
           .appName('SparkByExamples.com')
           .config("spark.jars", "mysql-connector-java-6.0.6.jar")
           .getOrCreate()

# Create DataFrame 
columns = ["id", "name","age","gender"]
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Write to SQL Table
sampleDF.write \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .mode("overwrite") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .save()

# Read from SQL Table
df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .load()

df.show()
