
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

#root
# |-- name: string (nullable = true)
# |-- subjects: array (nullable = true)
# |    |-- element: array (containsNull = true)
# |    |    |-- element: string (containsNull = true)
#
#+-------+-----------------------------------+
#|name   |subjects                           |
#+-------+-----------------------------------+
#|James  |[[Java, Scala, C++], [Spark, Java]]|
#|Michael|[[Spark, Java, C++], [Spark, Java]]|
#|Robert |[[CSharp, VB], [Spark, Python]]    |
#+-------+-----------------------------------+



from pyspark.sql.functions import explode
df.select(df.name,explode(df.subjects)).show(truncate=False)


#+-------+------------------+
#|name   |col               |
#+-------+------------------+
#|James  |[Java, Scala, C++]|
#|James  |[Spark, Java]     |
#|Michael|[Spark, Java, C++]|
#|Michael|[Spark, Java]     |
#|Robert |[CSharp, VB]      |
#|Robert |[Spark, Python]   |
#+-------+------------------+


from pyspark.sql.functions import flatten
df.select(df.name,flatten(df.subjects)).show(truncate=False)



#+-------+-------------------------------+
#|name   |flatten(subjects)              |
#+-------+-------------------------------+
#|James  |[Java, Scala, C++, Spark, Java]|
#|Michael|[Spark, Java, C++, Spark, Java]|
#|Robert |[CSharp, VB, Spark, Python]    |
#+-------+-------------------------------+
