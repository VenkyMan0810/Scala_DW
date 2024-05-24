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

    val weatherRDD = sc.textFile(s"gs://${BUCKET_NAME}/uk-traffic/weather.txt")

    val processedWeatherRDD = weatherRDD.map{
      inString =>
        val regionRegEx = """(?<=of )(.*)(?= on)""".r
        val dateRegEx = """(?<= on )(.*)(?= at)""".r
        val timeRegEx = """(?<=at )(.*)(?= the)""".r
        val conditionRegEx = """(?<=reported: )(.*)""".r
        val region = regionRegEx.findFirstIn(inString)
        val date = dateRegEx.findFirstIn(inString)
        val time = timeRegEx.findFirstIn(inString)
        val condition = conditionRegEx.findFirstIn(inString)
        (region, date, time, condition)
    }

    val weatherDF = processedWeatherRDD.toDF("local_authority_ons_code", "date", "time", "condition").withColumn("date", to_date(col("date"), "dd/MM/yyyy")).withColumn("timestamp_id", concat(date_format(col("date"), "yyyyMMdd"), col("time").substr(0,2)))
    val dimRegionDF = spark.read.format("delta").load("/tmp/delta/dim_regions")
    val windowId = Window.partitionBy("weather_id").orderBy(col("cnt").desc)

    val dimWeatherDF = weatherDF
      .join(dimRegionDF, "local_authority_ons_code")
      .withColumn("timestamp", to_timestamp(unix_timestamp($"date", "yyyy-MM-dd") + unix_timestamp($"time", "HH:mm")))
      .withColumn("previous_date", lag("timestamp", 1).over(Window.partitionBy("region_auth_id").orderBy("timestamp_id")))
      .withColumn("next_date", lead("timestamp", 1).over(Window.partitionBy("region_auth_id").orderBy("timestamp_id")))
      .withColumn("from_date", coalesce(date_add($"previous_date", (datediff($"timestamp", $"previous_date") / 2).cast("int")), $"timestamp"))
      .withColumn("to_date", coalesce(date_add($"timestamp", (datediff($"next_date", $"timestamp") / 2).cast("int")), $"timestamp"))
      // .select(monotonically_increasing_id().alias("weather_id"), $"condition", $"from_date", $"to_date")
      .select(concat($"timestamp_id", $"region_auth_id").alias("weather_id"), $"condition", $"from_date", $"to_date")
    // .select($"timestamp", $"region_auth_id", $"condition", $"from_date", $"to_date")
    // .where("region_auth_id = 1").orderBy($"timestamp")

    dimWeatherDF.write.format("delta").mode("overwrite").save("/tmp/delta/dim_weather")

  }

}
