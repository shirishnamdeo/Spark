from pyspark import SparkContext
from pyspark.context import SparkContext
# Both imports are working on the cluster!!


spark2-shell --conf spark.ui.port=<port-number>


# Need to create a spark contest if not runnig from pyspark shell.
# https://github.com/apache/spark/blob/master/python/pyspark/context.py#L60
sc = SparkContext(master="local", appName="first app")


sc.textFile("hdfs:///apps/<database>/testdir/filename.txt")
# This is working

sc.textFile("file:///cs/<database>/filename.txt")
# Isn't working


logData = sc.textFile().cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
