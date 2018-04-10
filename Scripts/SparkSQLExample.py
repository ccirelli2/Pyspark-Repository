#CREATE A SIMPLE DATAFRAME USING SQL CONTEXT

from pyspark import SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql.types import StringType

# Create a Spark & SQL Context
sc = SparkContext()
sqlContext = SQLContext(sc)

# Create a Dataframe From Created RDD 
'''
RDD = sc.parallelize([('Jon', 30), ('Howee', 45), ('Steve', 20)])
SQL_Dataframe = RDD.toDF(['Name', 'age'])
print('######', SQL_Dataframe.collect())
'''

# Create Dataframe From CSV RDD

File = r'/home/ccirelli2/Desktop/Scalable_Analytics/DataFiles/citibike.csv'
RDD_CSV = sc.textFile(File)
RDD_split = RDD_CSV.flatMap(lambda x: x.split(','))
SQL_Dataframe = RDD_split.toDF['cartodb_id,the_geom,tripduration,starttime,stoptime,start_station_id,start_station_name,start_station_latitude,start_station_longitude,end_station_id,end_station_name,end_station_latitude,end_station_longitude,bikeid,usertype,birth_year,gender', '1,,801,2015-02-01 00:00:00+00,2015-02-01 00:14:00+00,521,8 Ave & W 31 St,40.75044999,-73.99481051,423,W 54 St & 9 Ave,40.76584941,-73.98690506,17131,Subscriber,1978,2']



print('#######', SQL_Dataframe.show())



'''
# Import a File
File = r'/home/ccirelli2/Desktop/Scalable_Analytics/DataFiles/citibike.csv'







# Create Text RDD
CSV_RDD = sc.textFile(File) 



dataframe = CSV_RDD.toDF(['cartodb_id, \
			the_geom, \
			tripduration, \
			starttime, \
			stoptime, \
			start_station_id, \
			start_station_name, \
			start_station_latitude, \
			start_station_longitude, \
			end_station_id, \
			end_station_name, \
			end_station_latitude, \
			end_station_longitude, \
			bikeid, \
			usertype, \
			birth_year, \
			gender'])

 





print('#######', CSV_RDD.take(10))


'''
