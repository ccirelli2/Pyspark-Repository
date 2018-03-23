# Import pypark

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Test.py')
sc = SparkContext(conf = conf)

# Lets program



Text = sc.textFile('bible.txt')
Count = Text.count()

print('##################', Count)






