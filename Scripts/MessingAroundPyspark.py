# CREATE SPARK CONTEXT AND CONF

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setMaster('local').setAppName('MessingAroundPyspark.py')
sc = SparkContext(conf = conf)


# Create RDDs

'DocketSheet\ Classification_70_02.22.2018.xlsx'
















