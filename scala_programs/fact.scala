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
      .withColumn("timestamp", to_timestamp(unix_timestamp(to_timestamp($"count_date")) + unix_timestamp($"hour", "H")))
      .withColumn("local_authority_ons_code", $"local_authoirty_ons_code")

    val dimRegionDF = spark.read.format("delta").load("/tmp/delta/dim_regions")
    val dimRoadDF = spark.read.format("delta").load("/tmp/delta/dim_road")
    val dimWeatherDF = spark.read.format("delta").load("/tmp/delta/dim_weather")
    val dimDirectionsDF = spark.read.format("delta").load("/tmp/delta/dim_directions")
    val dimTimeDF = spark.read.format("delta").load("/tmp/delta/dim_time")

    val factDF = mainDF.join(broadcast(dimRegionDF), "local_authority_ons_code")
      .join(broadcast(dimRoadDF), "road_category")
      .join(dimWeatherDF, to_date(dimWeatherDF("to_date")) === to_date(mainDF("timestamp")) && hour(dimWeatherDF("to_date")) === hour(mainDF("timestamp")), "left")
      .join(broadcast(dimDirectionsDF), "direction_of_travel")
      .join(dimTimeDF, "timestamp_id")
      .groupBy("region_auth_id", "road_id", "weather_id", "direction_id", "timestamp_id")
      .agg(sum($"pedal_cycles").cast("int").alias("sum_pedal_cycles"),
        sum($"two_wheeled_motor_vehicles").cast("int").alias("sum_two_wheeled_motor_vehicles"),
        sum($"cars_and_taxis").cast("int").alias("sum_cars_and_taxis"),
        sum($"buses_and_coaches").cast("int").alias("sum_buses_and_coaches"),
        sum("lgvs").cast("int").alias("sum_lgvs"),
        sum($"all_hgvs").cast("int").alias("sum_hgvs"),
        sum($"all_motor_vehicles").cast("int").alias("sum_all_motor_vehicles"))
      .withColumn("sum_all_vehicles", $"sum_all_motor_vehicles" + $"sum_pedal_cycles")

    factDF.write.format("delta").mode("overwrite").save("/tmp/delta/fact")

  }

}
