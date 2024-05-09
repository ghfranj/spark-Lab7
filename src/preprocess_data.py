from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, countDistinct
def preprocess_data(spark, file_path):
    df = spark.read.csv(file_path, inferSchema=True)
    df.printSchema()
    df = df.na.drop()
    selected_features = df.columns
    df = df.select(selected_features).na.drop()
    string_indexers = [StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="keep") for column in
                       selected_features if df.select(column).dtypes[0][1] == 'string']
    string_indexed_df = df
    for indexer in string_indexers:
        string_indexed_df = indexer.fit(string_indexed_df).transform(string_indexed_df)

    columns_to_drop = [column for column in selected_features if string_indexed_df.select(column).dtypes[0][1] == 'string']

    df = string_indexed_df.drop(*[col(column) for column in columns_to_drop])
    return df
