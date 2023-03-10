
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()
df.show(truncate=False)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()

from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()
    
from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()

from pyspark.sql.functions import cume_dist    
df.withColumn("cume_dist",cume_dist().over(windowSpec)) \
   .show()

from pyspark.sql.functions import lag    
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

from pyspark.sql.functions import lead    
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()
    
windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()




# 2.1 row_number Window Function
# row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)
# Yields below output.
# 
# 
# +-------------+----------+------+----------+
# |employee_name|department|salary|row_number|
# +-------------+----------+------+----------+
# |James        |Sales     |3000  |1         |
# |James        |Sales     |3000  |2         |
# |Robert       |Sales     |4100  |3         |
# |Saif         |Sales     |4100  |4         |
# |Michael      |Sales     |4600  |5         |
# |Maria        |Finance   |3000  |1         |
# |Scott        |Finance   |3300  |2         |
# |Jen          |Finance   |3900  |3         |
# |Kumar        |Marketing |2000  |1         |
# |Jeff         |Marketing |3000  |2         |
# +-------------+----------+------+----------+
# 2.2 rank Window Function
# rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.


"""rank"""
from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()
# Yields below output.
# 
# 
# +-------------+----------+------+----+
# |employee_name|department|salary|rank|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|   1|
# |        James|     Sales|  3000|   1|
# |       Robert|     Sales|  4100|   3|
# |         Saif|     Sales|  4100|   3|
# |      Michael|     Sales|  4600|   5|
# |        Maria|   Finance|  3000|   1|
# |        Scott|   Finance|  3300|   2|
# |          Jen|   Finance|  3900|   3|
# |        Kumar| Marketing|  2000|   1|
# |         Jeff| Marketing|  3000|   2|
# +-------------+----------+------+----+
# This is the same as the RANK function in SQL.
# 
# 2.3 dense_rank Window Function
# dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.


"""dens_rank"""
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()
# Yields below output.
# 
# 
# +-------------+----------+------+----------+
# |employee_name|department|salary|dense_rank|
# +-------------+----------+------+----------+
# |        James|     Sales|  3000|         1|
# |        James|     Sales|  3000|         1|
# |       Robert|     Sales|  4100|         2|
# |         Saif|     Sales|  4100|         2|
# |      Michael|     Sales|  4600|         3|
# |        Maria|   Finance|  3000|         1|
# |        Scott|   Finance|  3300|         2|
# |          Jen|   Finance|  3900|         3|
# |        Kumar| Marketing|  2000|         1|
# |         Jeff| Marketing|  3000|         2|
# +-------------+----------+------+----------+
# This is the same as the DENSE_RANK function in SQL.
# 
# 2.4 percent_rank Window Function

""" percent_rank """
from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()
# Yields below output.
# 
# 
# +-------------+----------+------+------------+
# |employee_name|department|salary|percent_rank|
# +-------------+----------+------+------------+
# |        James|     Sales|  3000|         0.0|
# |        James|     Sales|  3000|         0.0|
# |       Robert|     Sales|  4100|         0.5|
# |         Saif|     Sales|  4100|         0.5|
# |      Michael|     Sales|  4600|         1.0|
# |        Maria|   Finance|  3000|         0.0|
# |        Scott|   Finance|  3300|         0.5|
# |          Jen|   Finance|  3900|         1.0|
# |        Kumar| Marketing|  2000|         0.0|
# |         Jeff| Marketing|  3000|         1.0|
# +-------------+----------+------+------------+
# This is the same as the PERCENT_RANK function in SQL.

# 2.5 ntile Window Function
# ntile() window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)


"""ntile"""
from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()
# Yields below output.


# +-------------+----------+------+-----+
# |employee_name|department|salary|ntile|
# +-------------+----------+------+-----+
# |        James|     Sales|  3000|    1|
# |        James|     Sales|  3000|    1|
# |       Robert|     Sales|  4100|    1|
# |         Saif|     Sales|  4100|    2|
# |      Michael|     Sales|  4600|    2|
# |        Maria|   Finance|  3000|    1|
# |        Scott|   Finance|  3300|    1|
# |          Jen|   Finance|  3900|    2|
# |        Kumar| Marketing|  2000|    1|
# |         Jeff| Marketing|  3000|    2|
# +-------------+----------+------+-----+
# This is the same as the NTILE function in SQL.

# 3. PySpark Window Analytic functions
# 3.1 cume_dist Window Function
# cume_dist() window function is used to get the cumulative distribution of values within a window partition.
# 
# This is the same as the DENSE_RANK function in SQL.


""" cume_dist """
from pyspark.sql.functions import cume_dist    
df.withColumn("cume_dist",cume_dist().over(windowSpec)) \
   .show()

# +-------------+----------+------+------------------+
# |employee_name|department|salary|         cume_dist|
# +-------------+----------+------+------------------+
# |        James|     Sales|  3000|               0.4|
# |        James|     Sales|  3000|               0.4|
# |       Robert|     Sales|  4100|               0.8|
# |         Saif|     Sales|  4100|               0.8|
# |      Michael|     Sales|  4600|               1.0|
# |        Maria|   Finance|  3000|0.3333333333333333|
# |        Scott|   Finance|  3300|0.6666666666666666|
# |          Jen|   Finance|  3900|               1.0|
# |        Kumar| Marketing|  2000|               0.5|
# |         Jeff| Marketing|  3000|               1.0|
# +-------------+----------+------+------------------+
# 3.2 lag Window Function
# This is the same as the LAG function in SQL.


"""lag"""
from pyspark.sql.functions import lag    
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

# +-------------+----------+------+----+
# |employee_name|department|salary| lag|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|null|
# |        James|     Sales|  3000|null|
# |       Robert|     Sales|  4100|3000|
# |         Saif|     Sales|  4100|3000|
# |      Michael|     Sales|  4600|4100|
# |        Maria|   Finance|  3000|null|
# |        Scott|   Finance|  3300|null|
# |          Jen|   Finance|  3900|3000|
# |        Kumar| Marketing|  2000|null|
# |         Jeff| Marketing|  3000|null|
# +-------------+----------+------+----+
# 3.3 lead Window Function
# This is the same as the LEAD function in SQL.


 """lead"""
from pyspark.sql.functions import lead    
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()

# +-------------+----------+------+----+
# |employee_name|department|salary|lead|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|4100|
# |        James|     Sales|  3000|4100|
# |       Robert|     Sales|  4100|4600|
# |         Saif|     Sales|  4100|null|
# |      Michael|     Sales|  4600|null|
# |        Maria|   Finance|  3000|3900|
# |        Scott|   Finance|  3300|null|
# |          Jen|   Finance|  3900|null|
# |        Kumar| Marketing|  2000|null|
# |         Jeff| Marketing|  3000|null|
# +-------------+----------+------+----+
# 4. PySpark Window Aggregate Functions
# In this section, I will explain how to calculate sum, min, max for each department using PySpark SQL Aggregate window functions and WindowSpec. When working with Aggregate functions, we don’t need to use order by clause.


windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()
# This yields below output
# 
# 
# +----------+------+-----+----+----+
# |department|   avg|  sum| min| max|
# +----------+------+-----+----+----+
# |     Sales|3760.0|18800|3000|4600|
# |   Finance|3400.0|10200|3000|3900|
# | Marketing|2500.0| 5000|2000|3000|
# +----------+------+-----+----+----+