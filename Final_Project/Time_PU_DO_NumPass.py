'''Ideas

1.) Tip amount as % of cost by number of customers
2.) Piquete travel times.
'''


# CREATE SPARK Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.master('local[4]').getOrCreate()

# IMPORT PYTHON PACKAGES
import pandas as pd
import os

# CREATE A DATAFRAME OF THE TAXI TRIP DATA
File_TripData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/yellow_tripdata_2017-02.csv'
df_TripData = my_spark.read.csv(File_TripData, header = True)

# CREATE A DATAFRAME OF THE LOCATION CODES
File_ZoneData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/taxi+_zone_lookup.csv'
df_Loc_Codes = my_spark.read.csv(File_ZoneData, header = True)


# Limit DataFrame to Number of Passengers, Pick-Up And Drop-Off Locations
PU_DO_NumPass = df_TripData.select(df_TripData['tpep_pickup_datetime'],
								   df_TripData['tpep_dropoff_datetime'],
								   df_TripData['passenger_count'],
								   df_TripData['total_amount'])

# Convert Limited DataFrame to an RDD
PU_DO_NumPass_RDD = PU_DO_NumPass.rdd.map(tuple)

# Convert DropOff & Pickup Times to Hours
'''Documentation
x[0][10:13] => Pickup Hour
x[1][10:13] => Dropoff Hour
x[2] 	    => Number of passengers
x[3]        => Cost

PU_DO_Times = PU_DO_NumPass_RDD.map(lambda x: (x[0][10:13], (x[1][10:13], x[2] ,x[3])))
print('@@@@@@@@',PU_DO_Times.take(5))
'''

# QUESTION 1:  WHAT IS THE FREQUENCY OF TRIPS BY HOUR?
#NumTripsByHour_RDD = PU_DO_NumPass_RDD.map(lambda x: (x[0][10:13],1)).reduceByKey(lambda x,y: x+y)
#print('########', NumTripsByHour_RDD.sortByKey(ascending = False).collect())


# QUESTION 2: DOES THE NUMBER OF PASSENGERS CHANGE THROUGHOUT THE DAY?
'''Documentation:
(x,())   => The count of the key (hour, number of passengers)
(,(x,y)  => Hour, number of persons.
'''
PU_DO_Times = PU_DO_NumPass_RDD.map(lambda x: ((x[0][10:13], x[2]), 1))\
.reduceByKey(lambda x,y: x+y)\
.map(lambda x: (x[1], x[0])).sortByKey(ascending = False)\
.map(lambda x: (x[0], x[1][0], x[1][1]))\
.coalesce(1).collect()
#.coalesce(1).saveAsTextFile('/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/Trips_Hour_NumPass')
#print('########', PU_DO_Times)

df = pd.DataFrame(PU_DO_Times)

def write_to_excel(dataframe, location, filename):
    os.chdir(location)
    writer = pd.ExcelWriter(filename+'.xlsx')
    dataframe.to_excel(writer, sheet_name = 'Data')
    writer.save()

target_dir = '/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/'

write_to_excel(df, target_dir, 'TripNumPassResults')
