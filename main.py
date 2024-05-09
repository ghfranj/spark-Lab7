# import findspark
# findspark.init('C:\SPARK')
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from src.preprocess_data import preprocess_data
from src.MySpark import MySpark
from pyspark.ml.feature import VectorAssembler

mySpark = MySpark('config.ini')
spark = mySpark.get_spark_session()

# Unload any existing data in Spark
spark.catalog.clearCache()

# Read CSV file and preprocess data
data_path = "data/en.openfoodfacts.org.products.csv"
df = preprocess_data(spark, data_path)
df.drop(df.columns[0])
# Store DataFrame data into Oracle using JDBC
df.printSchema()
df.write.format("jdbc").options(
    url='jdbc:oracle:thin:@oracle-db:1521/ORCLPDB1',

    driver='oracle.jdbc.driver.OracleDriver',

    dbtable='foodfacts',

    user='sys as sysdba',

    password='Oracle_123'
).mode("overwrite").save()
# Unload DataFrame data from Spark
spark.catalog.clearCache()

# Read data again from Oracle using JDBC
df_from_oracle = spark.read.format("jdbc").options(
    url='jdbc:oracle:thin:@oracle-db:1521/ORCLPDB1',

    driver='oracle.jdbc.driver.OracleDriver',

    dbtable='foodfacts',

    user='sys as sysdba',

    password='Oracle_123'
).load()
feature_columns = df_from_oracle.columns
# Train KMeans model
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Transform your DataFrame to include the features column
df_from_oracle = assembler.transform(df_from_oracle)
kmeans = KMeans().setK(5).setSeed(1)
df_from_oracle.printSchema()
print("columns len:", len(df_from_oracle.columns))
model = kmeans.fit(df_from_oracle)

# Make predictions
predictions = model.transform(df_from_oracle)
evaluator = ClusteringEvaluator()

wssse = evaluator.evaluate(predictions)

print("Within Set Sum of Squared Errors = " + str(wssse))
# Show the result
centers = model.clusterCenters()
print("Cluster Centers:")
for center in centers:
    print(center)

# Stop SparkSession
spark.stop()