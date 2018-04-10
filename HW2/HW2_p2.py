# HW2 - QUESTION 2
'''
Task: 	Get the top 10 words for each rating. 
HINT: 	use (Rating, Word)
Return:	1 star rating : --, --, --)
'''


# CREATE SPARK CONTEXT

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster('local').setAppName('HW2_p2.py')
sc = SparkContext(conf = conf)

# IMPORT PYTHON PACKAGES
import string

# LOAD FILE
Text_RDD = sc.textFile('/home/ccirelli2/Desktop/Scalable_Analytics/HW2/Amazon_Comments.csv')

# CREATE KEY VALUE PAIR 
'''Documentation
	1.) Text_RDD.map(lambda x: x.split('^')):  Split the text on '^'
	2.) map(lambda x: (x[-1], x[5].lower())):  Return the Rating + Text, convert text to lowercase
	3.) map(lambda x: (x[0], x[1].split(' '))):Return Rating & Text tokenized
	4.) flatMapValues(lambda x: (x[0], x[1])): Ensure KeyValue pair structure
	5.) filter(lambda x: x[1] not in string.punctuation): Strip punctuation from text
	6.) filter(lambda x: x[1] != 'i').persist(): Strip token 'i' that shows up in all ratings. 
	7.) persist():  Persist the result of this RDD
'''

Create_KeyValuePair = Text_RDD.map(lambda x: x.split('^')).map(lambda x: (x[-1], x[5].lower())).map(lambda x: (x[0], x[1].split(' '))).flatMapValues(lambda x: (x[0], x[1])).filter(lambda x: x[1] not in string.punctuation).filter(lambda x: x[1] != 'i').persist()

# Get Top Words by Star
'''Documentation
	1.) Create_KeyValuePair.filter(lambda x: '1.00' in x): Limit to a single rating
	2.) map(lambda x: (x[1], 1)): Return a key value pair of (Token, 1)
	3.) reduceByKey(lambda x,y: x+y): Reduce key value pair (add like keys together)
	4.) map(lambda x: (x[1], x[0])): Change the shape of the keyvalue pair to (value, key)
	5.) sortByKey(ascending = False): Sort from highest to lowest on the value
	6.) map(lambda x: x[1]): Reshape to return just the key. 
	7.) take(10): take the first tenwords. 
'''

OneStar = Create_KeyValuePair.filter(lambda x: '1.00' in x).map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending = False).map(lambda x: x[1]).take(10)

TwoStar = Create_KeyValuePair.filter(lambda x: '2.00' in x).map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending = False).map(lambda x: x[1]).take(10)

ThreeStar = Create_KeyValuePair.filter(lambda x: '3.00' in x).map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending = False).map(lambda x: x[1]).take(10)

FourStar = Create_KeyValuePair.filter(lambda x: '4.00' in x).map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending = False).map(lambda x: x[1]).take(10)

FiveStar = Create_KeyValuePair.filter(lambda x: '5.00' in x).map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending = False).map(lambda x: x[1]).take(10)

print('Top 10 Words')
print('1 star rating :', OneStar)
print('2 star rating :', TwoStar)
print('3 star rating :', ThreeStar)
print('4 star rating :', FourStar)
print('5 star rating :', FiveStar)














