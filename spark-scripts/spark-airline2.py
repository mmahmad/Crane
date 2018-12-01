from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("spark_airline2") \
    .master("spark://172.22.158.8:7077") \
    .getOrCreate()

df = spark.read.csv("/home/mmahmad3/cs425mp4/input/airline-5k-with-header.csv")
df.cache()

df.createOrReplaceTempView("airlines")
sqlDF = spark.sql("SELECT _c8, _c9 FROM airlines WHERE _c16='LAX' and _c17='LAS'")
sqlDF.show()

spark.stop()