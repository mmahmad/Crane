# from __future__ import print_function
# from pyspark import SparkContext, SparkConf
# import sys
# from pyspark.streaming import StreamingContext

# '''
# words = sc.parallelize (
#    ["scala", 
#    "java", 
#    "hadoop", 
#    "spark", 
#    "akka",
#    "spark vs hadoop", 
#    "pyspark",
#    "pyspark and spark",
#    "scala",
#    "java",
#    "hadoop",
#    "spark",
#    "akka",
#    "spark vs hadoop",
#    "pyspark",
#    "pyspark and spark"
#    "scala",
#    "java",
#    "hadoop",
#    "spark",
#    "akka",
#    "spark vs hadoop",
#    "pyspark",
#    "pyspark and spark"]*10000)
# '''

# # def main(sc):
# #    words = sc.parallelize (
# #    ["scala",
# #    "java",
# #    "hadoop",	
# #    "spark",
# #    "akka",
# #    "spark vs hadoop",
# #    "pyspark",
# #    "pyspark and spark",
# #    "scala",
# #    "java",
# #    "hadoop",
# #    "spark",
# #    "akka",
# #    "spark vs hadoop",
# #    "pyspark",
# #    "pyspark and spark"
# #    "scala",
# #    "java",
# #    "hadoop",
# #    "spark",
# #    "akka",
# #    "spark vs hadoop",
# #    "pyspark",
# #    "pyspark and spark"]*10000
# # )
# #    counts = words.count()
# #    print "Number of elements in RDD -> %i" % (counts)

# if __name__ == "__main__":
# 	if len(sys.argv) != 3:
# 		# print "Usage: network_wordcount.py <hostname> <port>"
# 		# print file=sys.stderr
# 		sys.exit(-1)
#    # configure Spark
# 	conf = SparkConf().setAppName("NETWORK_WORDCOUNT5")
# 	conf = conf.setMaster("spark://172.22.158.8:7077")
# 	sc = SparkContext(conf=conf)
# 	ssc = StreamingContext(sc, 1)
# 	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
# 	counts = lines.flatMap(lambda line: line.split(" "))\
# 				  .map(lambda word: (word, 1))\
# 				  .reduceByKey(lambda a, b: a+b)

# 	counts.saveAsTextFiles('NETWORK_WORDCOUNT10')
# 	# counts.pprint()
# 	# arr = counts.collect()
# 	# with open('spark_job_output.txt', 'a+') as outfile:
# 	# 	outfile.write(arr)

# 	# print counts
   
# 	ssc.start()
# 	ssc.awaitTermination()
#    # Execute main()
#    # main(sc)

from pyspark import SparkContext
sc = SparkContext("spark://172.22.158.8:7077", "count app")
words = sc.parallelize (
   ['scala',
   'java',
   'hadoop',
   'spark',
   'akka',
   'spark vs hadoop',
   'pyspark',
   'pyspark and spark',
   'scala',
   'java',
   'hadoop',
   'spark',
   'akka',
   'spark vs hadoop',
   'pyspark',
   'pyspark and spark'
   'scala',
   'java',
   'hadoop',
   'spark',
   'akka',
   'spark vs hadoop',
   'pyspark',
   'pyspark and spark']*10000
)
counts = words.count()
print 'Number of elements in RDD -> %i' % (counts)