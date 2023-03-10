
from pyspark.sql.types import StringType, ArrayType
arrayCol = ArrayType(StringType(),False)


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()



#root
# |-- name: string (nullable = true)
# |-- languagesAtSchool: array (nullable = true)
# |    |-- element: string (containsNull = true)
# |-- languagesAtWork: array (nullable = true)
# |    |-- element: string (containsNull = true)
# |-- currentState: string (nullable = true)
# |-- previousState: string (nullable = true)
#+----------------+------------------+---------------+------------+-------------+
#|            name| languagesAtSchool|languagesAtWork|currentState|previousState|
#+----------------+------------------+---------------+------------+-------------+
#|    James,,Smith|[Java, Scala, C++]|  [Spark, Java]|          OH|           CA|
#|   Michael,Rose,|[Spark, Java, C++]|  [Spark, Java]|          NY|           NJ|
#|Robert,,Williams|      [CSharp, VB]|[Spark, Python]|          UT|           NV|
#+----------------+------------------+---------------+------------+-------------+




from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

#+----------------+------+
#|            name|   col|
#+----------------+------+
#|    James,,Smith|  Java|
#|    James,,Smith| Scala|
#|    James,,Smith|   C++|
#|   Michael,Rose,| Spark|
#|   Michael,Rose,|  Java|
#|   Michael,Rose,|   C++|
#|Robert,,Williams|CSharp|
#|Robert,,Williams|    VB|
#+----------------+------+



from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

#+--------------------+
#|         nameAsArray|
#+--------------------+
#|    [James, , Smith]|
#|   [Michael, Rose, ]|
#|[Robert, , Williams]|
#+--------------------+


from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()
#+----------------+--------+
#|            name|  States|
#+----------------+--------+
#|    James,,Smith|[OH, CA]|
#|   Michael,Rose,|[NY, NJ]|
#|Robert,,Williams|[UT, NV]|
#+----------------+--------+



from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()

#+----------------+--------------+
#|            name|array_contains|
#+----------------+--------------+
#|    James,,Smith|          true|
#|   Michael,Rose,|          true|
#|Robert,,Williams|         false|
#+----------------+--------------+
