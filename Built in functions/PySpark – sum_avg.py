
from pyspark.sql.functions import sum, col, desc
df.groupBy("state") \
  .agg(sum("salary").alias("sum_salary")) \
  .filter(col("sum_salary") > 100000)  \
  .sort(desc("sum_salary")) \
  .show()

#+-----+----------+
#|state|sum_salary|
#+-----+----------+
#|   NY|    252000|
#|   CA|    171000|
#|   NV|    166000|
#+-----+----------+



df.createOrReplaceTempView("EMP")
spark.sql("select state, sum(salary) as sum_salary from EMP " +
          "group by state having sum_salary > 100000 " + 
          "order by sum_salary desc").show()



df.groupBy("state").sum("salary").show()
#+-----+-----------+
#|state|sum(salary)|
#+-----+-----------+
#|   NJ|      91000|
#|   NV|     166000|
#|   CA|     171000|
#|   DE|      99000|
#|   NY|     252000|
#+-----+-----------+



# Group by using by giving alias name.
from pyspark.sql.functions import sum
dfGroup=df.groupBy("department") \
          .agg(sum("bonus").alias("sum_salary"))

#+-----+----------+
#|state|sum_salary|
#+-----+----------+
#|NJ   |91000     |
#|NV   |166000    |
#|CA   |171000    |
#|DE   |99000     |
#|NY   |252000    |
#+-----+----------+



# Filter after group by
dfFilter=dfGroup.filter(dfGroup.sum_salary > 100000)
dfFilter.show()

#+-----+----------+
#|state|sum_salary|
#+-----+----------+
#|   NV|    166000|
#|   CA|    171000|
#|   NY|    252000|
#+-----+----------+



# Sory by on group by column
from pyspark.sql.functions import asc
dfFilter.sort("sum_salary").show()

#+-----+----------+
#|state|sum_salary|
#+-----+----------+
#|   NV|    166000|
#|   CA|    171000|
#|   NY|    252000|
#+-----+----------+



# Sort by descending order.
from pyspark.sql.functions import desc
dfFilter.sort(desc("sum_salary")).show()

#+-----+----------+
#|state|sum_salary|
#+-----+----------+
#|   NY|    252000|
#|   CA|    171000|
#|   NV|    166000|
#+-----+----------+
