# CREATE SPARK CONTEXT
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
conf = SparkConf().setMaster('local').setAppName('Ch5_Loading_Savings.py')
sc = SparkContext(conf = conf)

# CHAPTER
'''
Explains the runtime architecture of a distributed Spark application,
Then discusses options for running Spark in distributed clusters.
Cluster Managers = Hadoop Yarn, Apache Mesos, and then Spark's own built in
cluster manager.
'''

# Spark runtime architecture:
'''
Distributed mode:  Spark uses a master/slave arcitecture with one central coordinator
and many distributed workers.
Driver: central coordinator
Executors:  Large number of distributed workers.
Spark application:  Driver and executors taken together are called a spark application.
Cluster Manager:  Spark application is launched using an external service called a
cluster manager.
Spark Shell:  When you launch a spark shell you've created a driver program. This is
becuase Spark Shell comes preloaded with a Spark Context called sc.

Driver Duties:
- converting user programs into units or tasks.
- scheduling tasks on executors: when executors are started they register themselves
with the driver.
-***The driver exposes information about the running Spark application through a web
interface, which by default is available at port 4040. http://localhost:4040

'''

# LAUNCHING A PROGRAM
'''
spark-submit:  used to launch programs.
main(): driver program invokes the main method specified by the user.
--master spark://host:7077:  specifies a cluster URL to connect to.
port:  By default the Spark Standalone masters use port 7077.
local:  Run a local mode with a single core.
local[n]:  Run in local mode with N cores.
syntax:  bin/spark-submit [options] <app jar | python file> [app options]
<app jar | python file>: refers to the entry poin tint oyour application

'''
