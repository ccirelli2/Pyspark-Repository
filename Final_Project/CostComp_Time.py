'''Ideas

1.) Tip amount as % of cost by number of customers
2.) Piquete travel times.
'''


# CREATE SPARK Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.master('local[4]').getOrCreate()

# IMPORT PYTHON PACKAGES
from dateutil import parser
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
								   df_TripData['fare_amount'])

# Convert Limited DataFrame to an RDD
PU_DO_NumPass_RDD = PU_DO_NumPass.rdd.map(tuple)

# QUESTION #1: WHAT IS THE AVERAGE COST PER PASSENGER (DOES IT INCREASE WITH THE NUMBER OF PASSENGERS?
'''Documentation
CALCULATION:
1.) Calculate the duration of a trip
2.) Divide the number of passengers by duration
3.) Create a key as the number of passengers.
4.) Accumulator to track count of rows.


VARIABLES:

Costs:
x[3]        => Cost
x[2]        => Number of Passengers
x[3] / x[2]	=> Cost per passenger

Trip Time:
x[0][10:16]						=> Pick-up Time
x[1][10:16] 					=> Dropoff Time
parser.parse(x[1][10:16])		=> Time stamp
Trip Time 						=> (x[1][10:16]) - parser.parse(x[0][10:16])
.total_seconds()				=> convert Trip time to seconds.

Cost / Time
Cost per passenger / Trip Time = Average Cost of Trip per minutet

RETURN RDD
(Number of passengers, Trip Duration min, Trip cost per minute)
'''

Limit_TripDuration_Greater0_RDD = PU_DO_NumPass_RDD.filter(lambda x: (parser.parse(x[1][10:16]) - parser.parse(x[0][10:16])).total_seconds() > 0)


PU_DO_Times_RDD = Limit_TripDuration_Greater0_RDD.map(lambda x:   (int(x[2]), 		# Number of passengers
												((((parser.parse(x[1][10:16])) - parser.parse(x[0][10:16]))).total_seconds()/60), # trip duration in minutes
												round((float(x[3]) /		# Total Cost divided by duration in minutes
												((((parser.parse(x[1][10:16])) - parser.parse(x[0][10:16]))).total_seconds()/60)),2)))

# Transform into Groups

def Transform_RDD_Duration_groups(RDD):
	if RDD[1] == 15 or RDD[1] < 15:
		return  RDD[0], '=<15min', RDD[2]
	elif RDD[1] == 30 or RDD[1] < 30 and RDD[1] > 15:
		return  RDD[0], '> 15, <=30min', RDD[2]
	elif RDD[1] == 45 or RDD[1] < 45 and RDD[1] > 30:
		return  RDD[0], '> 30, =<45_min', RDD[2]
	elif RDD[1] == 60 or RDD[1] < 60 and RDD[1] > 45:
		return  RDD[0], '> 45, =<60_min', RDD[2]
	elif RDD[1] > 60:
		return  RDD[0], '> 60min', RDD[2]

Time_Groups_RDD = PU_DO_Times_RDD.map(Transform_RDD_Duration_groups)

One_Passenger_TimeGroup_Cost = Time_Groups_RDD.filter(lambda x: x[0] == 1).map(lambda x: (x[1],x[2]))

One_Passenger_AverageCost_ByTripDuration = One_Passenger_TimeGroup_Cost.mapValues(lambda x: (x,1))\
																	   .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))

print('@@@@@@@@@@', One_Passenger_AverageCost_ByTripDuration.take(1))



def write_to_excel(dataframe, location, filename):
    os.chdir(location)
    writer = pd.ExcelWriter(filename+'.xlsx')
    dataframe.to_excel(writer, sheet_name = 'Data')
    writer.save()

target_dir = '/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/'

#write_to_excel(df, target_dir, 'TripNumPassResults')
