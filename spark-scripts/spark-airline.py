from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("spark_airlines") \
    .master("spark://172.22.158.8:7077") \
    .getOrCreate()

# logData = spark.read.text('/home/mmahmad3/cs425mp4/input/tweets-5k.csv').cache()

# df = spark.read.option("header", "false").csv("/home/mmahmad3/cs425mp4/input/tweets-5k-with-header.csv")
df = spark.read.csv("/home/mmahmad3/cs425mp4/input/airline-5k-with-headers.csv")
df.cache()
# df.show()
# milCyDf = df.loc[df['username'] == '@mileycyrus']

# newDf = df.filter(df['_c4'] == '2Hood4Hollywood').show()

df.createOrReplaceTempView("airlines")
sqlDF = spark.sql("SELECT count(*) FROM airlines WHERE _c16='LAX' and _c17='LAS'")
sqlDF.show()



# numAs = logData.filter(logData.value.contains('@mileycyrus')).count()

# print("Lines with @mileycyrus: %i" % (numAs))
# numAs.pprint()

spark.stop()