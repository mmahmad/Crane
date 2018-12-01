"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "/home/mmahmad3/cs425mp4/input/tweets.csv"  # Should be some file on your system
spark = SparkSession.builder.appName("TweetsCount").master('spark://172.22.158.8:7077').getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('@mileycyrus')).count()
# numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with @mileycyrus: %i" % (numAs))

spark.stop()