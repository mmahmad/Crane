'''
Copied from stream-wordcount.py
'''

from __future__ import print_function
from pyspark import SparkContext, SparkConf
import sys
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
	# if len(sys.argv) != 3:
		# print "Usage: network_wordcount.py <hostname> <port>"
		# print file=sys.stderr
		# sys.exit(-1)
   # configure Spark
	conf = SparkConf().setAppName("NETWORK_WORDCOUNT5")
	conf = conf.setMaster("spark://172.22.158.8:7077")
	sc = SparkContext(conf=conf)
	ssc = StreamingContext(sc, 1)

	lines = ssc.textFileStream('/home/mmahmad3/cs425mp4/input/tweets/')
	# counts = lines.flatMap(lambda line: line.split(","))\
	# 			  .map(lambda word: (word, 1))\
	# 			  .reduceByKey(lambda a, b: a+b)

	counts = lines.map(lambda word: (word[4], 1))

	# counts.saveAsTextFiles('NETWORK_WORDCOUNT10')
	counts.pprint()
	# arr = counts.collect()
	# with open('spark_job_output.txt', 'a+') as outfile:
	# 	outfile.write(arr)

	# print counts
   
	ssc.start()
	ssc.awaitTermination()
   # Execute main()
   # main(sc)
