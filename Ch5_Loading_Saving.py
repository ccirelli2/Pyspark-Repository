# CREATE SPARK CONTEXT

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Ch5_Loading_Savings.py')
sc = SparkContext(conf = conf)

# Create Text RDD
Text = sc.textFile('Moby_Dick.txt')
Tokenize = Text.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).collect()

# Saving Text RDD
Text_tokenized = sc.parallelize(Tokenize)

Text_tokenized.saveAsTextFile('Test Save Text File')


