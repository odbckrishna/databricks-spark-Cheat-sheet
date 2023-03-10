# 1. Create PySpark MapType
# In order to use MapType data type first, you need to import it from pyspark.sql.types.MapType and use MapType() constructor to create a map object.


from pyspark.sql.types import StringType, MapType
mapCol = MapType(StringType(),StringType(),False)
MapType Key Points:

# The First param keyType is used to specify the type of the key in the map.
# The Second param valueType is used to specify the type of the value in the map.
# Third parm valueContainsNull is an optional boolean type that is used to specify if the value of the second param can accept Null/None values.
# The key of the map won’t accept None/Null values.
# PySpark provides several SQL functions to work with MapType.
# 2. Create MapType From StructType
# Let’s see how to create a MapType by using PySpark StructType & StructField, StructType() constructor takes list of StructField, StructField takes a fieldname and type of the value.


from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])
# Now let’s create a DataFrame by using above StructType schema.


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.printSchema()
df.show(truncate=False)
df.printSchema() yields the Schema and df.show() yields the DataFrame output.


# root
#  |-- Name: string (nullable = true)
#  |-- properties: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
# 
# +----------+-----------------------------+
# |Name      |properties                   |
# +----------+-----------------------------+
# |James     |[eye -> brown, hair -> black]|
# |Michael   |[eye ->, hair -> brown]      |
# |Robert    |[eye -> black, hair -> red]  |
# |Washington|[eye -> grey, hair -> grey]  |
# |Jefferson |[eye -> , hair -> brown]     |
# +----------+-----------------------------+
# 3. Access PySpark MapType Elements
# Let’s see how to extract the key and values from the PySpark DataFrame Dictionary column. Here I have used PySpark map transformation to read the values of properties (MapType column)


df3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
df3.printSchema()
df3.show()

# root
#  |-- name: string (nullable = true)
#  |-- hair: string (nullable = true)
#  |-- eye: string (nullable = true)
# 
# +----------+-----+-----+
# |      name| hair|  eye|
# +----------+-----+-----+
# |     James|black|brown|
# |   Michael|brown| null|
# |    Robert|  red|black|
# |Washington| grey| grey|
# | Jefferson|brown|     |
# +----------+-----+-----+
# Let’s use another way to get the value of a key from Map using getItem() of Column type, this method takes a key as an argument and returns a value.


df.withColumn("hair",df.properties.getItem("hair")) \
  .withColumn("eye",df.properties.getItem("eye")) \
  .drop("properties") \
  .show()

df.withColumn("hair",df.properties["hair"]) \
  .withColumn("eye",df.properties["eye"]) \
  .drop("properties") \
  .show()
# 4. Functions
# Below are some of the MapType Functions with examples.
# 
# 4.1 – explode

from pyspark.sql.functions import explode
df.select(df.name,explode(df.properties)).show()

# +----------+----+-----+
# |      name| key|value|
# +----------+----+-----+
# |     James| eye|brown|
# |     James|hair|black|
# |   Michael| eye| null|
# |   Michael|hair|brown|
# |    Robert| eye|black|
# |    Robert|hair|  red|
# |Washington| eye| grey|
# |Washington|hair| grey|
# | Jefferson| eye|     |
# | Jefferson|hair|brown|
# +----------+----+-----+
# 4.2 map_keys() – Get All Map Keys

from pyspark.sql.functions import map_keys
df.select(df.name,map_keys(df.properties)).show()

# +----------+--------------------+
# |      name|map_keys(properties)|
# +----------+--------------------+
# |     James|         [eye, hair]|
# |   Michael|         [eye, hair]|
# |    Robert|         [eye, hair]|
# |Washington|         [eye, hair]|
# | Jefferson|         [eye, hair]|
# +----------+--------------------+
# In case if you wanted to get all map keys as Python List. WARNING: This runs very slow.


from pyspark.sql.functions import explode,map_keys
keysDF = df.select(explode(map_keys(df.properties))).distinct()
keysList = keysDF.rdd.map(lambda x:x[0]).collect()
print(keysList)
#['eye', 'hair']
#4.3 map_values() – Get All map Values

from pyspark.sql.functions import map_values
df.select(df.name,map_values(df.properties)).show()

# +----------+----------------------+
# |      name|map_values(properties)|
# +----------+----------------------+
# |     James|        [brown, black]|
# |   Michael|             [, brown]|
# |    Robert|          [black, red]|
# |Washington|          [grey, grey]|
# | Jefferson|             [, brown]|
# +----------+----------------------+