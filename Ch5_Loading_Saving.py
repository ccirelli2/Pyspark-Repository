# CREATE SPARK CONTEXT

from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
conf = SparkConf().setMaster('local').setAppName('Ch5_Loading_Savings.py')
sc = SparkContext(conf = conf)

# Create Text RDD
'''
Text = sc.textFile('Moby_Dick.txt')
Tokenize = Text.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).collect()
'''

# Saving Text RDD
'''Note that when you get a result from one RDD, say using an action, you need to create 
another RDD object, as show below, in order to use the RDD object functions (should be obvious)'''
'''
Text_tokenized = sc.parallelize(Tokenize)
Text_tokenized.saveAsTextFile('Test Save Text File')
'''

# Load CSV Using SQL Dataframe 
'''You can also load as a textfile but the instructions are unclear
sql = SQLContext(sc)
df = sql.read.csv("citibike.csv")
print('###########', df.head(30))
'''

# Try creating an RDD of a pandas dataframe

import pandas as pd
'''Not clear if this actually worked'''
'''
df = pd.read_csv('citibike.csv')
Pandas_RDD = sc.parallelize(df)
print('###########', Pandas_RDD.collect())
'''










