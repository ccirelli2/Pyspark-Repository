
# IMPORT SPARK AND CREATE CONTEXT

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('ClassLoadSaveData.py')
sc = SparkContext(conf = conf)


# Import Libraries
import pandas 
import os
import operator


# Create a RDD of a text file

Text = sc.textFile('bible.txt')
Tokenize = Text.flatMap(lambda x: x.split(' '))
Enumerate = Tokenize.map(lambda x: (x,1))
print('#############', Enumerate.collect())



# Loading & Saving Data:
'''
Data Sources:
- Apache Hive - look this up.
- Cassandra, Hbase, Elasticsearch, and JDBC databases. 

File Formats:
- SequenceFiles - A common Hadoop file format used for key/value data
**you can load an entire directory of text files. See presentation.  You would use an 
asterics.  


FileSystem:

Local/Regular:  'file://'path   * why this syntax?
		*Its important to remember that you have access to both your local and the 
		hadoop file system.
Hadoop:		
		'hdfs://master:port/path'> you need to specify the path like this to get
		access to the hadoop file system.  Presumably this is to get access to data
		saved in the different nodes. 

Apache Hive:
		from pyspark.sql iport HiveContext
		hiveCtx = HiveContext(sc)  > way to create a Hive Context. 
		rows = hiveCtx.sql('SELECT name, age FROM users')
		firstRow =rows.first()
		print( firstRow.name)


JSON data Using Hive (Tweet Data)
	tweets = hiveCtx.jsonFile('tweets.json')
	tweets.registerTempTable('tweets')
	results = hiveCtx.sql('SELECT user.name, text FROM tweets')


Accumulator Syntax
acc1 = sc.accumulator(0)  > zero is the starting point for the accumulation. 
global acc1 > this is similar to calling global(name) in normal python.  You are making the
		name global in normal python. I guess the point is to make the name global 
		across allnodes. acc1 += 1
		See presentation.  The reason that you call the accumulator is to distribute
		the task across the nodes. 

Datatypes =     Accumulators only support int, float, 	


Broadcast:
- cach1 = sc.broadcast(RDD1) > allow all nodes to see and utilize this RDD. 

Pre-Partition Operators:
- mapPartitions(), mapPartitionsWithIndex(), foreachPartition()
-Look into these and research them.  It looks like you are just running a function on a part oof the entire dataset. 
- Apparently using the mapPartition and usinga form loop is 2-3 times faster than calling map
on the entirety of the dataset. 

Numeric RDD Operations 
- See book.  count, mean, sum, max, min, variance, stdev, etc. 











'''























