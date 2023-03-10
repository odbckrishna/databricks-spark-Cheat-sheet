# 1. PySpark DataFrame filter() Syntax
# Below is syntax of the filter function. condition would be an expression you wanted to filter.


# filter(condition)
# Before we start with examples, first let’s create a DataFrame. Here, I am using a DataFrame with StructType and ArrayType columns as I will also be covering examples with struct and array types as-well.


from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)
# This yields below schema and DataFrame results.


# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- languages: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)
# 
# +----------------------+------------------+-----+------+
# |name                  |languages         |state|gender|
# +----------------------+------------------+-----+------+
# |[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
# |[Anna, Rose, ]        |[Spark, Java, C++]|NY   |F     |
# |[Julia, , Williams]   |[CSharp, VB]      |OH   |F     |
# |[Maria, Anne, Jones]  |[CSharp, VB]      |NY   |M     |
# |[Jen, Mary, Brown]    |[CSharp, VB]      |NY   |M     |
# |[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
# +----------------------+------------------+-----+------+
# 2. DataFrame filter() with Column Condition
# Use Column with the condition to filter the rows from DataFrame, using this you can express complex condition by referring column names using dfObject.colname
# 

# Using equals condition
df.filter(df.state == "OH").show(truncate=False)

# +----------------------+------------------+-----+------+
# |name                  |languages         |state|gender|
# +----------------------+------------------+-----+------+
# |[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
# |[Julia, , Williams]   |[CSharp, VB]      |OH   |F     |
# |[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
# +----------------------+------------------+-----+------+

# not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False) 
df.filter(~(df.state == "OH")) \
    .show(truncate=False)
# Same example can also written as below. In order to use this first you need to import from pyspark.sql.functions import col


#Using SQL col() function
from pyspark.sql.functions import col
df.filter(col("state") == "OH") \
    .show(truncate=False) 
# 3. DataFrame filter() with SQL Expression
# If you are coming from SQL background, you can use that knowledge in PySpark to filter DataFrame rows with SQL expressions.


#Using SQL Expression
df.filter("gender == 'M'").show()
#For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()
# 4. PySpark Filter with Multiple Conditions
# In PySpark, to filter() rows on DataFrame based on multiple conditions, you case use either Column with a condition or SQL expression. Below is just a simple example using AND (&) condition, you can extend this with OR(|), and NOT(!) conditional expressions as needed.


# //Filter multiple condition
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)  
# This yields below DataFrame results.


# +----------------------+------------------+-----+------+
# |name                  |languages         |state|gender|
# +----------------------+------------------+-----+------+
# |[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
# |[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
# +----------------------+------------------+-----+------+
# 5. Filter Based on List Values
# If you have a list of elements and you wanted to filter that is not in the list or in the list, use isin() function of Column class and it doesn’t have isnotin() function but you do the same using not operator (~)


#Filter IS IN List values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
# +--------------------+------------------+-----+------+
# |                name|         languages|state|gender|
# +--------------------+------------------+-----+------+
# |    [James, , Smith]|[Java, Scala, C++]|   OH|     M|
# | [Julia, , Williams]|      [CSharp, VB]|   OH|     F|
# |[Mike, Mary, Will...|      [Python, VB]|   OH|     M|
# +--------------------+------------------+-----+------+

# Filter NOT IS IN List values
#These show all records with NY (NY is not part of the list)
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li)==False).show()
# 6. Filter Based on Starts With, Ends With, Contains
# You can also filter DataFrame rows by using startswith(), endswith() and contains() methods of Column class. For more examples on Column class, refer to PySpark Column Functions.


# Using startswith
df.filter(df.state.startswith("N")).show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|      [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
|[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
|  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
+--------------------+------------------+-----+------+

#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()
# 7. PySpark Filter like and rlike
# If you have SQL background you must be familiar with like and rlike (regex like), PySpark also provides similar methods in Column class to filter similar values using wildcard characters. You can use rlike() to filter by checking values case insensitive.


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()
# +---+----------+
# | id|      name|
# +---+----------+
# |  5|Rames rose|
# +---+----------+

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
# +---+------------+
# | id|        name|
# +---+------------+
# |  2|Michael Rose|
# |  4|  Rames Rose|
# |  5|  Rames rose|
# 8. Filter on an Array column
# When you want to filter rows from DataFrame based on value present in an array collection column, you can use the first syntax. The below example uses array_contains() from Pyspark SQL functions which checks if a value contains in an array if present it returns true otherwise false.


from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)     
# This yields below DataFrame results.


# +----------------+------------------+-----+------+
# |name            |languages         |state|gender|
# +----------------+------------------+-----+------+
# |[James, , Smith]|[Java, Scala, C++]|OH   |M     |
# |[Anna, Rose, ]  |[Spark, Java, C++]|NY   |F     |
# +----------------+------------------+-----+------+
# 9. Filtering on Nested Struct columns
# If your DataFrame consists of nested struct columns, you can use any of the above syntaxes to filter the rows based on the nested column.


#   //Struct condition
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 
# This yields below DataFrame results


# +----------------------+------------+-----+------+
# |name                  |languages   |state|gender|
# +----------------------+------------+-----+------+
# |[Julia, , Williams]   |[CSharp, VB]|OH   |F     |
# |[Mike, Mary, Williams]|[Python, VB]|OH   |M     |
# +----------------------+------------+-----+------+
# 10. Source code of PySpark where filter

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]
        
arrayStructureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('languages', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True)
         ])


df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

df.filter(df.state == "OH") \
    .show(truncate=False)

df.filter(col("state") == "OH") \
    .show(truncate=False)    
    
df.filter("gender  == 'M'") \
    .show(truncate=False)    

df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)        

df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)        

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 