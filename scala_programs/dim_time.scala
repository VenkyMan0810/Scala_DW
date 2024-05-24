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
      .withColumn("timestamp_id", concat(date_format(col("count_date"), "yyyyMMdd"), col("hour")).cast("int"))

    val dimTimeDF = mainDF.select(col("timestamp_id").cast("long"))
      .withColumn("year", col("timestamp_id").substr(0,4).cast("int"))
      .withColumn("month", col("timestamp_id").substr(5,2).cast("int"))
      .withColumn("day", col("timestamp_id").substr(7,2).cast("int"))
      .withColumn("hour", col("timestamp_id").substr(9,2).cast("int")).distinct()

    dimTimeDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_time")

  }

}
