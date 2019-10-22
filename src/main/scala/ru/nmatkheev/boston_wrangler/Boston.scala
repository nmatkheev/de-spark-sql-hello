package ru.nmatkheev.boston_wrangler

import java.nio.file.Paths
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel


object Boston extends App {
  val log = LogManager.getLogger("main_logger")

  val (mainData,mapping,output) = (args(0),args(1), args(2))
  log.warn("--------------------------------------------------")
  log.warn(s"files: $mainData, $mapping =>>> saving to $output")
  log.warn("--------------------------------------------------")

  val conf = new SparkConf()

  val spark = SparkSession.builder
    .appName("Wine Parsing")
    .master("local[*]")
    .config(conf)
    .getOrCreate

  Parser.run(spark, mainData, mapping, output)
}

object Parser {

  def run(spark: SparkSession, main_data_p: String, map_data_p: String, outpath: String) = {
    import spark.implicits._

    val data = spark.read.option("header", true).csv(main_data_p)
      .withColumnRenamed("OFFENSE_CODE", "OFFENSE_CODE1")
      .filter('DISTRICT.isNotNull && 'OFFENSE_CODE1.isNotNull)
      .withColumn("OFFENSE_CODE", 'OFFENSE_CODE1.cast(IntegerType))
      .drop("OFFENSE_CODE1")

    val mapper = spark.read.option("header", true).csv(map_data_p)
      .withColumn("crime_type", split('NAME, "-")(0))
      .select('CODE.as("OFFENSE_CODE").cast(IntegerType), 'crime_type.as("CRIME_TYPE"))
      .dropDuplicates("OFFENSE_CODE")

    val w1 = Window.partitionBy("DISTRICT", "YEAR", "MONTH").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val w2 = Window.partitionBy("DISTRICT", "CRIME_TYPE").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val w3 = Window.partitionBy("DISTRICT").orderBy('raw_frequency_crime_type.desc_nulls_last)

    val alldata = data.join(broadcast(mapper), Seq("OFFENSE_CODE"))
      .select('DISTRICT, 'INCIDENT_NUMBER, 'Lat, 'Long, 'CRIME_TYPE, 'YEAR, 'MONTH)
      .withColumn("raw_crimes_monthly", count('INCIDENT_NUMBER).over(w1))
      .withColumn("raw_frequency_crime_type", count('INCIDENT_NUMBER).over(w2))

    val crime_rank = alldata
      .select('DISTRICT, 'CRIME_TYPE, 'raw_frequency_crime_type)
      .dropDuplicates("DISTRICT", "CRIME_TYPE")
      .withColumn("city_rank", dense_rank().over(w3))
//      .withColumn("city_rank", row_number().over(w3))
      .filter('city_rank <= 3)
      .orderBy('DISTRICT, 'city_rank)
      .repartition(50)

    crime_rank.persist(StorageLevel.MEMORY_ONLY_SER)

    val median = expr("percentile_approx(raw_crimes_monthly, 0.5)")

    val main_agg_p1 = alldata.drop("CRIME_TYPE")
      .join(broadcast(crime_rank), Seq("DISTRICT"))
      .groupBy('DISTRICT)
      .agg(
        concat_ws(",", collect_set('CRIME_TYPE)).as("frequent_crime_types"),
        count('INCIDENT_NUMBER).as("crimes_total"),
        median.as("crimes_monthly"),
        avg('Lat).as("lat"),
        avg('Long).as("lng")
      )

    main_agg_p1.write.parquet(Paths.get(outpath, "result_agg1.pqt").toString)
  }
}
