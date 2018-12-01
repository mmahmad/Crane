from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("batch_tweets") \
    .master("spark://172.22.158.8:7077") \
    .getOrCreate()

df = spark.read.csv('/home/mmahmad3/cs425mp4/input/tweets.csv')
