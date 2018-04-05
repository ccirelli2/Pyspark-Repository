# IMPORT SPARK AND CREATE CONTEXT

from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster('local').setAppName('SCA_FreqDist.py')
sc = SparkContext(conf = conf)


# IMPORT LIBRARIES 
from operator import add
import pandas as pd
import os


# Create a Function to Concatenate all of the SCA Files into a single document
os.chdir(r'/home/ccirelli2/Desktop/Starr/filings_text_legal_team')
List_files = os.listdir()


File1 = List_files[0]
File2 = List_files[1]
RDD1 = sc.textFile(r'/home/ccirelli2/Desktop/Starr/filings_text_legal_team/*.txt')
'''
RDD2 = sc.textFile(File2)
CoGroup = RDD1.cogroup(RDD2)
print('############', CoGroup.collect())


print('#########', File1)
'''

print('#########', RDD1.collect())

