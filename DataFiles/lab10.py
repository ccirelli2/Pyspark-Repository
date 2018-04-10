from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("lab10")

sc = SparkContext(conf=conf)

lines = sc.textFile("Amazon_Comments.csv")

#result = lines.map(analyse_eachline)
#def analyse_eachline(eachline):
#       return eachline.split("^")[-2]
#print result.collect()

#result2 = lines.mapPartitions(analyse_multiline)
def analyse_multiline(multiline):
        for eachline in multiline:
                yield eachline.split("^")[-2]
result2 = lines.mapPartitions(analyse_multiline)
result2.saveAsTextFile("output")
