from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, countDistinct
def preprocess_data(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True, sep='\t')
    # Select relevant features
    selected_features = ["code", "product_name", "brands", "categories", "countries",
                         "nutriscore_grade", "nova_group", "pnns_groups_1", "pnns_groups_2", "ecoscore_grade"]

    # Drop rows with missing values in selected features
    df = df.select(selected_features).na.drop()
    # for col_name in df.columns:
    #     unique_count = df.select(countDistinct(col(col_name))).collect()[0][0]
    #     print(f"Column '{col_name}' has {unique_count} unique values.")
    # String indexing for string-type columns
    string_indexers = [StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="keep") for column in
                       selected_features  if df.select(column).dtypes[0][1] == 'string']
    string_indexed_df = df
    for indexer in string_indexers:
        string_indexed_df = indexer.fit(string_indexed_df).transform(string_indexed_df)

    # Vectorize features
    indexed_feature_cols = [column + "_index" for column in selected_features if df.select(column).dtypes[0][1] == 'string']  # Use indexed columns
    assembler = VectorAssembler(inputCols=indexed_feature_cols, outputCol="features")
    df = assembler.transform(string_indexed_df)

    columns_to_drop = [column for column in selected_features if df.select(column).dtypes[0][1] == 'string']

    # Dropping string columns
    df = df.drop(*[col(column) for column in columns_to_drop])
    df.printSchema()
    return df
