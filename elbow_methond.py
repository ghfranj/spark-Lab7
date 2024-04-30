from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read BSON with PySpark") \
    .getOrCreate()

# Path to your BSON file
bson_file_path = "E:/Downloads/openfoodfacts-mongodbdump/dump/off/products.bson"

# Read BSON file into a DataFrame
df = spark.read.format("bson").load(bson_file_path)
