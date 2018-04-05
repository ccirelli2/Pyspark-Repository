# Import pypark

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Test.py')
sc = SparkContext(conf = conf)

# Example Count Words in Text
'''
Text = sc.textFile('bible.txt')
Count = Text.count()
#print('##################', Count)
'''

# Fold

from operator import add
'''
Num = sc.parallelize([1,2,3,4,5])
Sum_count = Num.fold(0, add)
print('###########', Sum_count)
'''

# Reduce
'''
Num = sc.parallelize([x for x in range(0,1000000000000)])
Num_reduce = Num.reduce(lambda x,y: x+y)
print('##############', Num_reduce)
'''


# Top
'''top() will return a set number of elements from your RDD results.  
Whereas take() returns a sampling of you results, top() will return them in 
order from top to bottom'''

'''
Num = sc.parallelize([1,2,3,4,5])
Num_square = Num.map(lambda x: x**2)
print('############', Num_square.top(3))
'''

# COMMON ACTIONS ON NUMBERS


# countByValue()
'''Number of times each element occurs in the RDD'''
'''
Num = sc.parallelize([1,2,3,4,3,2,3,4,5,2,3,4,5,6,1])
Count_by_value = Num.countByValue()
print('##############', Count_by_value)
'''

# PERSISTING AN RDD
'''Note that you should call persist() prior to the first action. Note that you are
persisting the RDD and not the result of an action.  So if you parallelize 1-4, that is
what you will persist, the list of numbers that now constitute the RDD'''

Num = sc.parallelize([1,2,3,4,5])
Num.persist()
Num_fold = Num.fold(0,add)
# Actions
print('###########', Num_fold)
print('###########', Num_fold*2)
print('###########', Num_fold/2)






































