 
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]

df =spark.createDataFrame(address,["id","address","state"])
df.show()

#Replace string
from pyspark.sql.functions import regexp_replace
df.withColumn('address', regexp_replace('address', 'Rd', 'Road')) \
  .show(truncate=False)

#Replace string
from pyspark.sql.functions import when
df.withColumn('address', 
      when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
     .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
     .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
     .otherwise(df.address)) \
     .show(truncate=False)   


#Replace values from Dictionary
stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
df2=df.rdd.map(lambda x: 
    (x.id,x.address,stateDic[x.state]) 
    ).toDF(["id","address","state"])
df2.show()

#Using translate
from pyspark.sql.functions import translate
df.withColumn('address', translate('address', '123', 'ABC')) \
  .show(truncate=False)

#Replace column with another column
from pyspark.sql.functions import expr
df = spark.createDataFrame([("ABCDE_XYZ", "XYZ","FGH")], ("col1", "col2","col3"))
df.withColumn("new_column",
              expr("regexp_replace(col1, col2, col3)")
              .alias("replaced_value")
              ).show()
  
#Overlay
from pyspark.sql.functions import overlay
df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
df.select(overlay("col1", "col2", 7).alias("overlayed")).show()
