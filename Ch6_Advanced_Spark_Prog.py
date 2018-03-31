# CREATE SPARK CONTEXT
from pyspark import SparkConf, SparkContext, SQLContext
conf = SparkConf().setMaster('local[*]').setAppName('Ch6_Advanced_Spark_Prog.py')
sc = SparkContext(conf= conf)
from operator import add

# CREATE A TEXT RDD
Text_file = sc.textFile('/home/ccirelli2/Desktop/Scalable_Analytics/Moby_Dick.txt').persist()

# TOKENIZE TEXT & GROUP BY KEY
'''
Tokenize_textfile = Text_file.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1))
Group_tokens = Tokenize_textfile.reduceByKey(add)
print('######', Group_tokens.take(10))
'''

# ACCUMULATORS (SHARED VARIABLES)
'''Provides a simple syntax for aggreating values from worker nodes back to the driver programself.
Common uses include counting events that occur during jobs for debugging purposes.  Below is an example
of a function to count blank lines in a text documentself.
Accumulators let us count errors or values as we load data, without having doing a separate filter() or reduce()'''

# Define your accumulator object.  Set it to 0
blanklines = sc.accumulator(0)
# Define object to add values to the accumulator
def get_blanklines(line):
    global blanklines
    if line == '':
        blanklines +=1
    return line.split(' ')
# Map the get_blanklines functiont to the RDD Text Object
Blanklines_results = Text_file.flatMap(get_blanklines)
# Call Count on the accumulator object to return the values

# Print Count
'''
print(Blanklines_results.count())
'''

# Save As Text File
'''
Blanklines_results.saveAsTextFile('/home/ccirelli2/Desktop/Scalable_Analytics/Blacklines')
'''

# Other Accumulators - Double, Long, Float
'''
 See spark documentation
'''


# BROADCAST VARIABLES
'''
Allows the program to efficiently send a large, read-only value to all the worker nodes for us in one or more Spark operations.
Ex:  Lookup table sent to all nodes.
Note:  Books explanation & examples were not all that clear.
'''
# Create a broadcast object to send a value or series of values to all worker nodes.
'''
singPrefixes = sc.broadcast(loadCallSignTable())

# Define the function to generate your value.
def processSignCount(sign_count, signPrefixes):
    country = lookupCountry(sign_count[0], signPrefixes.value)
    count = sign_count[1]
    return (country, count)
'''


# WORKING ON PER-PARTITION BASIS
'''
Spark has a per-partition versions of map and foreach to help reduce the cost of these operations by letting you run code only once for each partition of an RDDself.
'''

# NUMERIC RDD OPERATIONS
'''
Spark's numeric operations are implemented with a streaming algorithm that allows for building up our model on element at a time.
The statistics are all computed with a single pass over the data and returned as a StatsCounter object by calling stats().
'''
List_values = [x for x in range(0,10)]
RDD_range_values = sc.parallelize(List_values)

print('#######', 'Mean', RDD_range_values.mean())
print('#######', 'Sum', RDD_range_values.sum())
print('#######', 'STDEV', round(RDD_range_values.stdev(), 2))
print('#######', 'STATS', RDD_range_values.stats())
