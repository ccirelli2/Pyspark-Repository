#SPARK DATAFRAME


from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':  # Is this defining a class?
	sc = SparkContext('local[2]', appName = 'NetworkWordCount')
	sc.setLogLevel('ERROR')
	ssc = StremingContext(sc, 1)

	# Create DStream from data source
	lines = ssc.socketTextStream('localhost', 9999)
	
	# Transformation and actions

	counts = lines.flatMap(lambda line: line.split(' '))\
		.map(lambda word: (word, 1))\
		.reduceByKey(lambda x,y: x+y)

	counts.print()

	# Starting the listening to the server

	ssc.start()
	ssc.awaitTerminal()

'''Running in terminal

Terminal1 = command nc -lk 9999
Terminal2 = spark-submit 'appName'

'''
	
	








