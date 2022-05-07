package dimensions
import org.apache.spark.sql.functions.{asc, col, date_format, monotonically_increasing_id, to_date, to_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Maker(private val spark:SparkSession){
  import spark.implicits._
  private var df: DataFrame = Seq.empty[Boolean].toDF()

  def this(spark:SparkSession,df:DataFrame){
    this(spark)
    setDF(df)
  }
  def this(spark:SparkSession,options:Map[String,String]){
    this(spark)
      val tempdf = spark.read.format(options("format"))
        .option("header","true")
        .option("delimiter",",")
        .option("infer.schema","true")
        .load(options("path"))
      setDF(tempdf)
  }
  def setDimensions(dimensions:Array[dimensionStruct]): Unit={
    for(x <- dimensions.indices){
      val dimension = dimensions(x)
      var tempdf = Seq.empty[Boolean].toDF()
      dimension.name match{
        case "time" => tempdf = timeDimension(dimension)
        case _ => tempdf = anyDimension(dimension)
      }
      val where = tempdf.columns.slice(0,tempdf.columns.length-1).map(x=>"d."+x+" == t."+x+" ")
      val filteredColumns = (for(x<- df.columns.indices if !dimension.attr.contains(df.columns(x))) yield df.columns(x)).toArray
      //val dfColumns = df.columns.map(x=>"d."+x)
      tempdf.createOrReplaceTempView("temp")
      val query = s"SELECT ${filteredColumns.mkString(",")}, ${dimension.name+"_id"} FROM df d " +
        s"JOIN temp t ON ${where.mkString(" AND ")}"
      setDF(spark.sql(query),format = false)
    }
  }
  def createFact(): Unit={
    df.write
      .mode("overwrite")
      .parquet("fact")
  }
  def drop(column: String): Unit ={
    setDF(df.drop(column.split(","):_*),format = false)
  }
  private def anyDimension(dimension: dimensionStruct): DataFrame={
    val dimensionDF = df.select(dimension.attr(0),dimension.attr.slice(1,dimension.attr.length):_*)
      .distinct()
      .withColumn(dimension.name+"_id",monotonically_increasing_id())
    dimensionDF.write
      .mode("overwrite")
      .option("header","true")
      .option("delimiter",",")
      .parquet(dimension.name)
    dimensionDF
  }
  private def timeDimension(dimension: dimensionStruct): DataFrame={
    val dimensionDF = df.select(dimension.attr(0)).distinct()
      .withColumn("time_id",monotonically_increasing_id())
    dimensionDF.write
      .mode("overwrite")
      .option("header","true")
      .option("delimiter",",")
      .parquet("time")
    dimensionDF
  }
  def setDF(df:DataFrame,format:Boolean =true): Unit ={
    if(format) this.df = Formatter.format(df)
    else this.df = df
    this.df.createOrReplaceTempView("df")
  }
  def show(): Unit ={
    df.show()
    println("Schema: "+ df.schema)
    println("Columns: " + df.columns.mkString(","))
    println("Rows: " + df.count())
  }
}
case class dimensionStruct(name:String,attr: Array[String])
