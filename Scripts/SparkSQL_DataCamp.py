# CHAPTER 1:  INITIATING THE SPARK SESSION AND CREATING A DATAFRAME
'''
1.) SparkSession.builder.getOrCreate():  
	Making a new spark session using getOrCreate ensures that if a Spark Session has
	already been created the code will return that session, otherwise it will 'creat
	a new one. 
2.) Catalog:
	Your spark session has an attribute called 'catalog', which lists all the data
	inside the cluster. One of the most useful methrods is .listTables()
3.) Read:
	SparkSession has a .read attribute which has several methods for reading 
	reading different data sources into Spark DataFrames. Using these you can 
	create a DataFrame frmo a .csv file.  
	Header:  If your csv file includes column headers be sure to set the header to
	true.   
4.) Show:
	Once you create your dataframe from the csv file you call call the .show()
	attribute to display your dataframe.  Similar to the pandas data frame .head()
	you can include within the () the number of rows that you would like to see.  

'''

# Create A Spark SQL Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.getOrCreate()


# Print the SparkSession Object
'''
print('#######', my_spark)
'''

# Print what tables are in your cluster
'''
print('######', my_spark.catalog.listTables())
'''

# Using the .read attribute, Create a DataFrame from a CSV File

File_path = r'/home/ccirelli2/Desktop/Scalable_Analytics/DataFiles/citibike.csv'
CSV_dataframe = my_spark.read.csv(File_path, header = True)


# Call Show on the newly create dataframe
'''
CSV_dataframe.show(10)
'''

# Run a Query on the Dataframe
'''
Query_top5 = 'FROM CSV_dataframe SELECT * LIMIT 5'
Top_5 = my_spark.sql(Query_top5)
Top_5.show()
'''


# CHAPTER 2: MINIPULATNG DATA
'''
1.) Calling a column:  
	Use df.colName
2.) Creating a New Column:
	.withColumn() takes two arguments.  i. String withthe name of the column
	ii. the new column itself.   
	Note:  SQL dataframes are immutable.  Therefore, to update a column a new df
	needs to be created. 
	Example:
		df = df.withColumn('newCol', df.oldCol + 1)
		Create a new column from the old column where each entry is equal
		to the entry in old col plus 1. 
	Notes:
		i. Define the name of the new DataFrame (since we have to return a 
                new one)
		ii. Call .withColumn on the existing DataFrame
		iii. Within the () define the name of the new column as a str
		iv. Call the existing column 'gender' as an attribute of the existing
		Dataframe. 		


3.) Create a Table
	spar.table(): This method allows you to create a table
'''

# Create a New Column Called Crap (Gender + 1)
'''
CSV_df2 = CSV_dataframe.withColumn('crap', CSV_dataframe.gender +1)
print('@@@@@@@@@@@',CSV_df2.show(10))
'''


# PART II: SQL in a Nutshell
'''
1.) Basic SQL:	SELECT (start) statement followed by the columns you	
		want to retreive the data frome. 
		FROM (end or middle) statement followed by the name of the table that
		contains the information.
		WHERE (end) conditional statement.  
2.) Print Col	print(df.columns) will print the column names. Use as reference. 

3.) GroupBy	Breaks data into groups

'''

# Print Column names
'''
print('@@@@@@@@@', CSV_dataframe.columns)
'''

# Query a table using the column names
''' Not working
query1 = 'SELECT cartodb_id, FROM CSV_dataframe';
Test = my_spark.sql(query1)
print(Test.show())
'''

# GroupBy Operation
'''
Group_statement = 'SELECT COUNT(*) FROM CSV_dataframe GROUP BY cartodb_id'
'''


# PART III:  FILTERING DATA USING RDD OPERATIONS
































