from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("spark_tweets") \
    .master("spark://172.22.156.10:7077") \
    .getOrCreate()

df = spark.read.csv("/home/mmahmad3/cs425mp4/input/tweets-5k-with-header.csv")
df.cache()

df.createOrReplaceTempView("tweets")
sqlDF = spark.sql("SELECT count(*) FROM tweets WHERE _c2 LIKE '%Mon%' OR _c5 LIKE '%@mileycyrus%'")
sqlDF.show()

spark.stop()

