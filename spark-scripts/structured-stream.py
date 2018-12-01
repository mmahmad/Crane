from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .master("spark://172.22.158.8:7077") \
    .getOrCreate()

userSchema = StructType().add("id", "string").add("tweet_id", "string").add("date", "string").add("query", "string").add("username", "string").add("body", "string")

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .option('sep', ',') \
    .schema(userSchema) \
    .csv("/home/mmahmad3/cs425mp4/input/tweets")  # Equivalent to format("csv").load("/path/to/directory")
    

    # .format("socket") \
    # .option("host", "172.22.158.8") \
    # .option("port", 9999) \
    # .load()

# Split the lines into words
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )

result = lines.select("date").where("body LIKE '%@mileycyrus%'")
# lines.createOrReplaceTempView("updates")

# Generate running word count
wordCounts = result.groupBy("date").count()

# count = result.count()
# newDF = spark.sql('SELECT count(*) FROM updates WHERE body LIKE "%@mileycyrus%"')

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .format("console") \
    .outputMode('complete') \
    .start() \
    .awaitTermination()