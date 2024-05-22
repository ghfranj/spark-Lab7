import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors

object DataPreprocessor {
  def preprocessData(spark: SparkSession, filePath: String): Dataset[Row] = {
    val df = spark.read.option("inferSchema", "true").csv(filePath)
    df.printSchema()

    val selectedFeatures = df.columns
    var tempDF = df.select(selectedFeatures.map(col): _*).na.drop()
    println("this is tempDF.printSchema():")
    tempDF.printSchema()

    // Define string columns
    val stringColumns = selectedFeatures.filter(column => tempDF.schema(column).dataType == StringType)

    // Define VectorAssembler to assemble all features
    val assembler = new VectorAssembler()
                      .setInputCols(tempDF.columns.filterNot(stringColumns.contains))
                      .setOutputCol("features")

    // Create a pipeline with all preprocessing stages
    val pipeline = new Pipeline().setStages(Array(assembler))

    // Fit the pipeline to the data
    val pipelineModel = pipeline.fit(tempDF)

    // Apply the pipeline to transform the data
    val finalDF = pipelineModel.transform(tempDF)

    // Convert the features vector column into an array of doubles
    val toArray = udf((v: Vector) => v.toArray)
    val dfWithArray = finalDF.withColumn("features_array", toArray(col("features")))

    // Explode the array column to create a row for each element in the array
    val explodedDF = dfWithArray.select(explode(col("features_array")).alias("element"))

    explodedDF.printSchema()
    explodedDF
  }
}
