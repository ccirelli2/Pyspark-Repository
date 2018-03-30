# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import pandas as pd
os.chdir(r'/home/ccirelli2/Desktop/Docket-Sheet-Classification/Modules')
import Step1_Module_Ngrams_FreqDist_version4_Ngrams as stp1

'''
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Test_pyspark_anaconda.py')
sc = SparkContext(conf = conf)
'''
#os.chdir(r'/home/ccirelli2/Desktop/Starr/filings_txt_data_final_tech_panthers')



List = [1,2,3,4]

List2 = [4,5,3,2]
List3 = [5,3,2,5]

print(list(zip(List, List2[1:], List3[2:])))