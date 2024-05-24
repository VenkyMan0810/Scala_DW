import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object UKTrafficAnalysis {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
//      .setMaster("local")
      .setAppName("UKTrafficAnalysis")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("UKTrafficAnalysis")
//      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val BUCKET_NAME = args(0)

    val mainDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/mainData*.csv")

    val dimRoadDF = mainDF.select($"road_category", $"road_type").distinct().withColumn("road_id", monotonically_increasing_id().cast("int"))

    dimRoadDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_road")

  }

}
