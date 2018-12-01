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

# logData = spark.read.text('/home/mmahmad3/cs425mp4/input/tweets-5k.csv').cache()

# df = spark.read.option("header", "false").csv("/home/mmahmad3/cs425mp4/input/tweets-5k-with-header.csv")
df = spark.read.csv("/home/mmahmad3/cs425mp4/input/tweets-5k-with-header.csv")
df.cache()
# milCyDf = df.loc[df['username'] == '@mileycyrus']

newDf = df.filter(df['username'] == '2Hood4Hollywood').show()
newDf.show()
# df.show()


# numAs = logData.filter(logData.value.contains('@mileycyrus')).count()

# print("Lines with @mileycyrus: %i" % (numAs))
# numAs.pprint()

spark.stop()

