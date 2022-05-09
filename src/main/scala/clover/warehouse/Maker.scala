package clover.warehouse

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}

/**Transforms a single source DataFrame or file into a data warehouse with a star schema
 * @param spark The current SparkSession
 */
class Maker(private val spark:SparkSession){
  import spark.implicits._
  private var df: DataFrame = Seq.empty[Boolean].toDF()

  /**Transforms a single source DataFrame into a data warehouse with a star schema
   * @param spark The current SparkSession
   * @param df A pre-made DataFrame
   */
  def this(spark:SparkSession,df:DataFrame){
    this(spark)
    setDF(df)
  }

  /**Transforms a single source file into a data warehouse with a star schema
   * @param spark The current SparkSession
   * @param options A map of options for reading the file
   */
  def this(spark:SparkSession,options:Map[String,String]){
    this(spark)
      val tempdf = spark.read.format(options("format"))
        .option("header","true")
        .option("delimiter",",")
        .option("infer.schema","true")
        .load(options("path"))
      if(options("filter").nonEmpty){
        val filteredDF = tempdf.filter(options("filter"))
        setDF(filteredDF)
      }
      else setDF(tempdf)
  }

  /**Creates the dimension tables from the original DataFrame
   * @param dimensions Array specifying what columns to use for which table
   * @param show Boolean for deciding if the dimension will be shown in the console
   */
  def setDimensions(dimensions:Array[dimensionStruct], show:Boolean=false): Unit={
    for(x <- dimensions.indices){
      val dimension = dimensions(x)
      var tempdf = Seq.empty[Boolean].toDF()
      dimension.name match{
        case "time" => tempdf = timeDimension(dimension)
        case _ => tempdf = anyDimension(dimension)
      }
      val where = tempdf.columns.slice(0,tempdf.columns.length-1).map(x=>"d."+x+" == t."+x+" ")
      val filteredColumns = (for(x<- df.columns.indices if !dimension.attr.contains(df.columns(x))) yield df.columns(x)).toArray
      tempdf.createOrReplaceTempView("temp")
      val query = s"SELECT ${filteredColumns.mkString(",")}, ${dimension.name+"_id"} FROM df d " +
        s"JOIN temp t ON ${where.mkString(" AND ")}"
      setDF(spark.sql(query),format = false)
      if(show) tempdf.show()
    }
  }

  /**Writes the fact to disk
   */
  def createFact(): Unit={
    df.write
      .mode("overwrite")
      .parquet("fact")
  }

  /**Drops the given column
   * @param column The column to drop
   */
  def drop(column: String): Unit ={
    setDF(df.drop(column.split(","):_*),format = false)
  }

  /**Creates a dimension table from the main DataFrame as long as it's not a time table
   * @param dimension Object with the name of the new table and the columns to use
   * @return A new dimension table
   */
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

  /**Creates a time dimension table from the main DataFrame
   * @param dimension Object with the name of the new table and the columns to use *Table name must be time*
   * @return A new time dimension table
   */
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

  /**Sets the main DataFrame to the one provided
   * @param df DataFrame to be set
   * @param format Whether the DataFrame needs to be formatted or not
   */
  def setDF(df:DataFrame,format:Boolean =true): Unit ={
    if(format) this.df = Formatter.format(df)
    else this.df = df
    this.df.createOrReplaceTempView("df")
  }

  /**Shows the first 20 rows, schema, column names, and number of rows in the console
   */
  def show(): Unit ={
    df.show()
    println("Schema: "+ df.schema)
    println("Columns: " + df.columns.mkString(","))
    println("Rows: " + df.count())
  }
}

/**Case class for defining the structure of the dimension tables
 * @param name The name of the dimension table
 * @param attr The columns from the main DataFrame to move to this table
 */
case class dimensionStruct(name:String,attr: Array[String])
