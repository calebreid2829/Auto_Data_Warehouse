import dimensions.Maker
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("Capstone Project")
      .config("spark.master", "local[*]")
      .config("spark.sql.legacy.timeParserPolicy","LEGACY")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")

    val options = Map("format"->"csv","path"->"../files/covid_19_data.csv")

    val dim = dimensions.dimensionStruct("time",Array("ObservationDate"))

    val maker = new Maker(spark,options)
    maker.setDimensions(1,Array(dim))

  }

}
