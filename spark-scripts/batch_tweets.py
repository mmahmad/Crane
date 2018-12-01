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

logData = spark.read.text('/home/mmahmad3/cs425mp4/input/tweets-5k.csv').cache()
numAs = logData.filter(logData.value.contains('@mileycyrus')).count()

print("Lines with @mileycyrus: %i" % (numAs))
numAs.pprint()

spark.stop()

