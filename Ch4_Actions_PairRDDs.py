# CREATE SPARK CONTEXT

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Ch4_Actions_PairRDDs.py')
sc = SparkContext(conf = conf)

# Create Text RDDs

# Moby Dick
Text_MobyDick = sc.textFile('Moby_Dick.txt')
Tokenize_Enumerated_MobyDick = Text_MobyDick.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1))
# Bible
Text_Bible = sc.textFile('bible.txt')
Tokenize_Enumerated_Bible = Text_Bible.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1))
# Print
'''
print('##################', 'MobyDick tokenized and enumerated', Tokenize_Enumerated_MobyDick.collect())
print('##############', '\n')
print('#############', 'Bible tokenized and enumerated', Tokenize_Enumerated_Bible.collect())
'''

# Example Union
RDD_union = Tokenize_Enumerated_MobyDick.union(Tokenize_Enumerated_Bible)
RDD_union_reduce = RDD_union.reduceByKey(lambda x,y: x+y) 
'''
print('######', 'RDD_union', RDD_union.take(10))
print('######', 'RDD_union_reduce', RDD_union_reduce.take(20))
'''

# Left Join RDDs
RDD_join_left = Tokenize_Enumerated_MobyDick.leftOuterJoin(Tokenize_Enumerated_Bible)
'''
print('#####', 'RDD_join_left', RDD_join_left.take(10))
'''

# Cogroup RDD'
'''Cogroup returns a tuple with one key and two values.  
Ex:  {key, (num values in RDD1, num values in RDD2)}'''
Cogroup_RDDs = Tokenize_Enumerated_MobyDick.cogroup(Tokenize_Enumerated_Bible)
'''
print('##############', Cogroup_RDDs)
'''

# Per Key Average
RDD_PerKeyAverage = Text_MobyDick.flatMap(lambda x: x.split(' '))


# Word Count
words = Text_Bible.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
'''
print('########', words.take(10))
'''

















 
