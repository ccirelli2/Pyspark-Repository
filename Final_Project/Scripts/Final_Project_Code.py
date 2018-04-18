# OBJECTIVE
'''
		Basic analysis of dataset. 

Question:	Why is SQL different in its setup, i.e. why do we not need to create
		a SparkContext?

'''

# CREATE SPARK Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.getOrCreate()


# CREATE A DATAFRAME OF THE TAXI TRIP DATA
File_TripData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/yellow_tripdata_2017-02.csv'

df_TripData = my_spark.read.csv(File_TripData, header = True)

Col_TripData = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']



# CREATE A DATAFRAME OF THE LOCATION CODES
File_ZoneData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/taxi+_zone_lookup.csv'
df_Loc_Codes = my_spark.read.csv(File_ZoneData, header = True) 


# INSPECT TRIP DATAFRAME
'''
print('@@@@@@@@@', df_TripData.show(5), '\n')
print('#########', df_TripData.columns)
'''

# INSPECT ZONE DATAFRAME
'''Note:  	This file includes the LocationID + Borough + Zone + Service Zone
		What do each mean and how can we possibly use each? '''
'''
print('@@@@@@@@', df_Loc_Codes.show(10), '\n')
print('#######', df_Loc_Codes.columns)
'''

# GET MOST FREQUENCT ROUTES BY ZONE
'''
1.) Get the most frequency routes for all zones
2.) Try to map the results for the top 10 
	Note:  It seems that it is more efficient to only map the LocationID
	once we actually obtain teh results (at which point we call the action). 
Note:	When calling rows you need to call the df each time. 
'''

# Limit DataFrame to Pick-Up And Drop-Off Locations
PU_DO_LocationCols = df_TripData.select(df_TripData['PULocationID'],df_TripData['DOLocationID'])

# Convert Limited DataFrame to an RDD
PU_DO_RDD = PU_DO_LocationCols.rdd.map(tuple)

# Calculate Count of Trips
PU_DO_Frequency = PU_DO_RDD.map(lambda x: ((x[0], x[1]),1)).reduceByKey(lambda x,y: x+y).persist()

# Get Top 10 Most Frequency Routes
PU_DO_Freq_Top10 = PU_DO_Frequency.map(lambda x: (x[1], x[0])).sortByKey(ascending = False).take(10)
print('#########', PU_DO_Freq_Top10)


# Limit Location RDD to LocationID / Borough
df_locID_Borough = df_Loc_Codes.select(df_Loc_Codes['LocationID'], df_Loc_Codes['Borough'])

'''
# Create a Function To Replace LocID with Bourough	
def matchBorough(RDD_Trips, RDD_Borough):
		
	for x in RDD_Trips:
		PU = ''
		DO = ''
		for y in RDD_Borough:
			if x[1][0] == y[0]:
				PU = y[0]
			else:
				PU = x[1][0]
			if x[1][1] == y[0]:
				DO = y[0]
			else:
				DO = x[1][1]
		
		return (x[0], (PU,DO))


print('########', matchBorough(PU_DO_Freq_Top10, df_locID_Borough))

'''
		
		
	
	



























