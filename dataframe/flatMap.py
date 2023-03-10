
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()



# root
#  |-- name: string (nullable = true)
#  |-- col: string (nullable = true)
# 
# +---------+------+
# |     name|   col|
# +---------+------+
# |    James|  Java|
# |    James| Scala|
# |  Michael| Spark|
# |  Michael|  Java|
# |  Michael|  null|
# |   Robert|CSharp|
# |   Robert|      |
# |Jefferson|     1|
# |Jefferson|     2|
# +---------+------+
