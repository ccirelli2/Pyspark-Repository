'''Pair RDDs
	Spark provides special operations on RDDs with key/value pairs.
	
'''
 
# IMPORT SPARK AND CREATE CONTEXT

from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster('local').setAppName('Ch4_KeyValuePairs_v2.py')
sc = SparkContext(conf = conf)


# IMPORT LIBRARIES 
from operator import add
import pandas as pd
import os

# CUSTOM FUNCTIONS

def write_to_excel(dataframe, location, filename):
    os.chdir(location)
    writer = pd.ExcelWriter(filename+'.xlsx')
    dataframe.to_excel(writer, sheet_name = 'Data')
    writer.save()


# Create Key Value Pairs & Reduce
'''
Text = sc.textFile('bible.txt').persist()

Tokenize = Text.flatMap(lambda x: x.split(' '))
Enumerate = Tokenize.map(lambda x: (x,1))
ReduceByKey = Enumerate.reduceByKey(add).collect()
print('###############', Tokenize.take(20))
print('##############', Enumerate.take(20))
print('##############', ReduceByKey)
'''

# Group By Key
'''
Tokenize_enumerate_groupbyKey = Text.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).filter(lambda x: len(x[0]) >5).reduceByKey(add).collect()
Index_length = len([x for x in range(0, len(Tokenize_enumerate_groupbyKey))])
# print('############', Tokenize_enumerate_groupbyKey.collect())
df = pd.DataFrame({}, index = [x for x in range(0,Index_length)])
df['Word'] = [x[0] for x in Tokenize_enumerate_groupbyKey]
df['Frequency'] = [x[1] for x in Tokenize_enumerate_groupbyKey]
df_sorted = df.sort_values(by = 'Frequency', ascending = True)
#print('###############', df_sorted.head(10))  
'''

# Create Bigram Frequency Distribution
'''
Tokenize_text = Text.flatMap(lambda x: x.split(' ')).collect() 
Tokenize_text_pos2 = Tokenize_text[1:]
Bigrams = [(x,y) for (x,y) in zip(Tokenize_text, Tokenize_text_pos2)] 	
Bigrams_gen = zip(Tokenize_text, Tokenize_text_pos2)
Bigrams_gen_RDD = sc.parallelize(Bigrams_gen)
Bigrams_gen_reduce = Bigrams_gen_RDD.map(lambda x: (x,1)).reduceByKey(add).collect()
#print('###################', Bigrams_gen_reduce)      
Index_length = len([x for x in range(0, len(Bigrams_gen_reduce))])       
df = pd.DataFrame({}, index = [x for x in range(0,Index_length)])
df['Word'] = [x[0] for x in Bigrams_gen_reduce]
df['Frequency'] = [x[1] for x in Bigrams_gen_reduce]
df_sorted = df.sort_values(by = 'Frequency', ascending = True)
write_to_excel(df_sorted, os.getcwd(), 'BigramFreqDist')  
'''

# Group Keys From Different RDD's
'''Note that I was unable to unpack the pyspark result iterable.''' 
'''
MobyDickText = sc.textFile('Moby_Dick.txt')
BibleText = sc.textFile('bible.txt')
MobyDick_tokenize_enumerate = MobyDickText.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(add)
BibleText_tokenize_enumerate = BibleText.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(add)

print('#############', MobyDick_tokenize_enumerate.take(10))
print('##############', BibleText_tokenize_enumerate.take(10))
CoGroup = MobyDick_tokenize_enumerate.cogroup(BibleText_tokenize_enumerate).collect()
'''
		
# Joins
'''
Cojoin = MobyDick_tokenize_enumerate.leftOuterJoin(BibleText_tokenize_enumerate).collect()
print('###########', Cojoin)
'''

# QUADGRAMS + OUTER JOIN = FREQ DIST

# Create RDD Text Files
MobyDickText = sc.textFile('Moby_Dick.txt')
BibleText = sc.textFile('bible.txt')

# Tokenize Text
MobyDick_token = MobyDickText.flatMap(lambda x: x.split(' ')).collect()
BibleText_token = BibleText.flatMap(lambda x: x.split(' ')).collect()

# Create Quadgrams using set
MobyDick_quadgram = zip(MobyDick_token, MobyDick_token[1:], MobyDick_token[2:], MobyDick_token[3:])
BibleText_quadgram = zip(BibleText_token, BibleText_token[1:], BibleText_token[2:], BibleText_token[3:])

# Recreate RDD's
MobyDick_quad_RDD = sc.parallelize(MobyDick_quadgram)
Bible_quad_RDD = sc.parallelize(BibleText_quadgram)

# Enumerate & Reduce Keys
MobyDick_enumerate_reduce = MobyDick_quad_RDD.map(lambda x: (x,1)).reduceByKey(add)
Bible_enumerate_reduce = Bible_quad_RDD.map(lambda x: (x,1)).reduceByKey(add)

# leftOuterJoin
OuterJoin_Moby_Bible = MobyDick_enumerate_reduce.join(Bible_enumerate_reduce).collect()

# Print Results
#print('###############', OuterJoin_Moby_Bible.collect()) 

# Write Results to Excel

Index_length = len([x for x in range(0, len(OuterJoin_Moby_Bible))])       
df = pd.DataFrame({}, index = [x for x in range(0,Index_length)])
df['Word'] = [x[0] for x in OuterJoin_Moby_Bible]
df['Frequency'] = [x[1] for x in OuterJoin_Moby_Bible]
write_to_excel(df, os.getcwd(), 'QuadgramsDist_MobyDick_Bible_Join')  
























