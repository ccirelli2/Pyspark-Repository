# TAKE A LOOK AT TAXI DATA

# IMPORT SPARK LIBRARIES / OBJECTS
'''Documentation
	- SparkContext: Entry point to the cluster. 
	- sql: sql library
	- SQLContext: Entry point to cluster via sql. 
'''
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg

# CREATE AS SPARK & SQL CONTEXT
'''
	1.) We create a SparkContext() as we normally would when working with RDDs. 
	2.) We pass our SparkContext() to the SQL Context. 

'''

sc = SparkContext()
sqlContext = sql.SQLContext(sc)











