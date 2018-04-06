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


# Create RDD

Ratings_data = sc.textFile('/home/ccirelli2/Desktop/Scalable_Analytics/HW2/Amazon_Comments.csv')

# Split Data:
'''retruns a list of lists in which each list constitutes a review and value within the list 0-6 are the entries'''
#Split_data = Ratings_data.flatMap(lambda x: x.split('\n')).map(lambda x: x.split('^')).collect()


# Tokenize Review Text
'''Text is in position 5'''

def Transform(Lists):
	return len(Lists[5].split(' ')[0].translate(None, string.punctuation)), Lists[6]


Split_data = Ratings_data.flatMap(lambda x: x.split('\n')).map(lambda x: x.split('^')).map(Transform)

#Test = map(Transform, Split_data)



print('#######', Split_data.take(1))





