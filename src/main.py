# import findspark
# findspark.init('C:\SPARK')
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

import requests
from MySpark import MySpark

mySpark = MySpark('config.ini')
spark = mySpark.get_spark_session()

# Unload any existing data in Spark
spark.catalog.clearCache()

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
# Read CSV file and preprocess data
data_path = "../datamart/data/en.openfoodfacts.org.products.csv"
if __name__ == '__main__':
    # # Unload DataFrame data from Spark


    spark.catalog.clearCache()
    response = None
    try:
        response = requests.get('http://localhost:9000/data')
    except:
        response = requests.get('http://datamart:9000/data')
        # df_from_oracle = spark.read.format("json").load("http://localhost:9000/data")

    # df_from_oracle = spark.read.format("json").load(response.json())
    df_from_oracle =  spark.createDataFrame(response.json())

    # Assuming 'element' is the feature column
    vector_assembler = VectorAssembler(inputCols=["element"], outputCol="features")
    df_from_oracle = vector_assembler.transform(df_from_oracle)
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