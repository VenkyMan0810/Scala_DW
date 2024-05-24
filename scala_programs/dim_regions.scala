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

    val authDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/authorities*.csv")
    val regionsDF = spark.read.option("header", true).csv(s"gs://${BUCKET_NAME}/uk-traffic/regions*.csv")

    val dimRegionDF = authDF.join(regionsDF, "region_ons_code").select($"local_authority_id".cast("int").alias("region_auth_id"), col("region_id").cast("int"), $"region_name", $"local_authority_name", $"local_authority_ons_code").orderBy("region_auth_id")
    dimRegionDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_regions")

  }

}
