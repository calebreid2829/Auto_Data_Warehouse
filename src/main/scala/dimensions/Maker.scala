package dimensions
import org.apache.spark.sql.functions.{asc, col, date_format, monotonically_increasing_id, to_date, to_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Maker(private val spark:SparkSession){
  import spark.implicits._
  private var df: DataFrame = Seq.empty[Boolean].toDF()
  private var fact: DataFrame = Seq.empty[Boolean].toDF("empty")

  def this(spark:SparkSession,df:DataFrame){
    this(spark)
    this.df = Formatter.format(df)
  }
  def this(spark:SparkSession,options:Map[String,String]){
    this(spark)
      val tempdf = spark.read.format(options("format"))
        .option("header","true")
        .option("delimiter",",")
        .option("infer.schema","true")
        .load(options("path"))
      df = Formatter.format(tempdf)
  }

  def setDimensions(numOfDimensions: Int, dimensions:Array[dimensionStruct]): Unit={
    for(x <- 0 until numOfDimensions){
      val dimension = dimensions(x)
      dimension.name match{
        case "time" => timeDimension(dimension)
      }

    }
  }
  private def timeDimension(dimension: dimensionStruct): DataFrame={
    val dimensionDF = df.select(dimension.attr.mkString(",")).distinct()
      .orderBy(asc("ObservationDate"))
      .withColumn("time_id",monotonically_increasing_id())
    dimensionDF.show()
    dimensionDF
  }
  def setDF(df:DataFrame): Unit ={
    this.df = Formatter.format(df)
  }
  def show(): Unit ={
    df.show()
    println("Schema: "+ df.schema)
    println("Columns: " + df.columns.mkString(","))
    println("Rows: " + df.count())

  }

}
case class dimensionStruct(name:String,attr: Array[String])
