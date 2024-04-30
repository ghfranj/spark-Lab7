import findspark
findspark.init('C:\SPARK')
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import ClusteringEvaluator
from preprocess_data import preprocess_data
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Clustering Model with PySpark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
# Read the data
data_path = "E:/Downloads/en.openfoodfacts.org.products.csv/en.openfoodfacts.org.products.csv"
df = preprocess_data(spark, data_path)


# Train KMeans model
kmeans = KMeans().setK(7).setSeed(1)
model = kmeans.fit(df)

# Make predictions
predictions = model.transform(df)
evaluator = ClusteringEvaluator()

wssse = evaluator.evaluate(predictions)

print("Within Set Sum of Squared Errors = " + str(wssse))
# Show the result
centers = model.clusterCenters()
print("Cluster Centers:")
for center in centers:
    print(center)

# Save the model
# model_path = "kmeans_model"
# model.write().save(model_path)

# Stop SparkSession
spark.stop()
