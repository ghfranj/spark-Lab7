import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .appName("DataPreprocessor")
                            .master("local[*]") // Change this to your master URL if needed
                            .getOrCreate()

    val filePath = "data/en.openfoodfacts.org.products.csv" // CSV file path
    val df: Dataset[Row] = DataPreprocessor.preprocessData(spark, filePath)
    println(s"Preprocessing completed.")

    // Store preprocessed data into Oracle database
    storePreprocessedDataToOracle(df)
    println(s"Data stored in Oracle.")

    // Start HTTP server to handle requests for data
    startHttpServer(spark)
    println(s"HTTP server started.")
  }

  def storePreprocessedDataToOracle(df: Dataset[Row]): Unit = {
    // JDBC URL for Oracle database
    val url = "jdbc:oracle:thin:@data_source:1521/ORCLPDB1"

    // Write preprocessed data to Oracle database
    df.write.format("jdbc")
      .options(
        Map(
          "url" -> url,
          "driver" -> "oracle.jdbc.driver.OracleDriver",
          "dbtable" -> "foodfacts",
          "user" -> "sys as sysdba",
          "password" -> "Oracle_123"
        )
      )
      .mode(SaveMode.Overwrite)
      .save()
  }

  def startHttpServer(spark: SparkSession): Unit = {
    import akka.actor.ActorSystem
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.server.Directives._
    import akka.stream.ActorMaterializer

    implicit val system = ActorSystem("DataServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val route = path("data") {
      get {
        // Read data from Oracle
        val oracleData = readDataFromOracle(spark)

        // Send the data as a response
        complete(oracleData.toJSON.collect().mkString("[", ",", "]"))
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(s"Server online at http://localhost:8080/")
  }

  def readDataFromOracle(spark: SparkSession): Dataset[Row] = {
    // JDBC URL for Oracle database
    val url = "jdbc:oracle:thin:@data_source:1521/ORCLPDB1"

    // Read data from Oracle table "foodfacts"
    val oracleData = spark.read.format("jdbc")
      .options(
        Map(
          "url" -> url,
          "driver" -> "oracle.jdbc.driver.OracleDriver",
          "dbtable" -> "foodfacts",
          "user" -> "sys as sysdba",
          "password" -> "Oracle_123"
        )
      )
      .load()
    oracleData
  }
}
