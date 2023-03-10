
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)



+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+



dataCollect = deptDF.collect()
print(dataCollect)



[Row(dept_name='Finance', dept_id=10), 
Row(dept_name='Marketing', dept_id=20), 
Row(dept_name='Sales', dept_id=30), 
Row(dept_name='IT', dept_id=40)]



for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))



#Returns value of First Row, First Column which is "Finance"
deptDF.collect()[0][0]


# # Let’s understand what’s happening on above statement.
# # 
# # deptDF.collect() returns Array of Row type.
# # deptDF.collect()[0] returns the first element in an array (1st row).
# # deptDF.collect[0][0] returns the value of the first row & first column.
# # In case you want to just return certain elements of a DataFrame, you should call PySpark select() transformation first.


dataCollect = deptDF.select("dept_name").collect()