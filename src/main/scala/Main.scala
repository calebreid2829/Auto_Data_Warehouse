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
    val options = Map("format"->"csv","path"->"../files/covid_19_data.csv","filter"->"Province_State != 'null' AND Province_State != 'Unknown'")
    val dim = dimensions.dimensionStruct("time",Array("ObservationDate"))
    val dim2 = dimensions.dimensionStruct("location",Array("Country_Region","Province_State"))
    val maker = new Maker(spark,options)
    maker.show()
    maker.setDimensions(Array(dim,dim2))
    maker.drop("SNo,Last_Update")
    maker.createFact()
  }

}
