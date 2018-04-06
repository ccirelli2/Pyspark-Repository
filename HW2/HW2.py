# QUESTION 1
'''
1.)	Get average length of words for each rating. 
	remove punctuation. 
	Result
		1 star rating: average length of comments --
'''


# CREATE SPARK CONTEXT

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster('local').setAppName('HW2.py')
sc = SparkContext(conf = conf)


# IMPORT PYTHON LIBRARIES 
import string


# CREATE RDD
'''Import CSV file as a Text RDD'''

Ratings_data = sc.textFile('/home/ccirelli2/Desktop/Scalable_Analytics/HW2/Amazon_Comments.csv')


# CREATE PAIR RDD
'''Documentation:
	1.) Create a top-level function called 'Transform' to 
		a.) Limit the return value to index 5 (text of review) and index 6 (rating)
		b.) Split the review text on spaces and thereby tokenizing it. 
		c.) Filter out any punctuation from the tokenized review text 
		d.) take the length of the list of tokenized text 
	2.) Return:  (Review, length text)
'''

def CreateKeyValuePair(Lists):
	punct = string.punctuation
	return Lists[6], len(list(filter(lambda x: x not in punct, Lists[5].split(' ')))) 


# TRANSFORM RDD
'''Documentation
	1.) Execute a series of RDD transformation functions to
		a.) Split text on each line
		b.) Split each line on '^'
		c.) Map our top-level function to this RDD
'''

Split_data = Ratings_data.flatMap(lambda x: x.split('\n')).map(lambda x: x.split('^')).map(CreateKeyValuePair)



# GROUP RDD BY KEY
Group_by_key = Split_data.groupByKey()



# GET AVERAGE LENGTH REVIEW

def Get_average(KeyValue):
	return KeyValue[0], round(sum(list(KeyValue[1])) / len(list(KeyValue[1])),2)

Average_length = Group_by_key.map(Get_average)


print('#######', Average_length.collect())

























