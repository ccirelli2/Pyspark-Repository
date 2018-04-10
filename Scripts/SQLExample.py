# SQL Dataframe Example



from pyspark import SparkContext
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg

sc = SparkContext()

sqlContext = sql.SQLContext(sc)

dataRDD = sc.parallelize([('Jim', 20), ('Anne', 31), ('Jonny', 30)])

# Create DataFrame

dataDf = dataRDD.toDF(['name', 'age'])

# Compute Average

#result1 = dataRDD.map(lambda x,y: (x,(y,1)))\
	#.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))\
	#.map(lambda x,y,z: (x,y/z))

# Use dataframe

result2 = dataDf.groupBy('name').agg(avg('age'))

#print('#######', result1.collect())
print('#######', result2.collect())





