package clover.warehouse

import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.{DataFrame, Row}

/**Object for formatting DataFrames automatically when Spark's infer.schema doesn't work as well as desired
 */
protected object Formatter {
  /**Formats the DataFrame to the correct data types
   * @param df DataFrame to format
   * @return The formatted DataFrame
   */
  def format(df: DataFrame): DataFrame = {
    /*val format = (str: String) => {
      if ("(\\d|\\d{2})([/-])(\\d|\\d{2})([/-])\\d{4}".r.findFirstMatchIn(str).nonEmpty)
        if ("/".r.findFirstMatchIn(str).nonEmpty) "MM/DD/YYYY"
        else "MM-DD-YYYY"
      else if ("\\d{4}([/-])(\\d|\\d{2})([/-])(\\d|\\d{2})".r.findFirstMatchIn(str).nonEmpty)
        if ("/".r.findFirstMatchIn(str).nonEmpty) "YYYY/MM/DD"
        else "YYYY-MM-DD"
    }*/
    var tempdf = df
    val dataTypes = dataTypeFind(tempdf.head, tempdf.columns)
    dataTypes.foreach(cl => {
      cl.collType match {
        case "datetime" => tempdf = tempdf.withColumn(cl.name, to_timestamp(col(cl.name), "MM/dd/yyyy HH:mm"))
        case "date" => tempdf = tempdf.withColumn(cl.name, to_date(col(cl.name), "MM/dd/yyyy"))
        case "int" => tempdf = tempdf.withColumn(cl.name, col(cl.name).cast("int"))
        case "double" =>
          try {
            tempdf = tempdf.withColumn(cl.name, col(cl.name).cast("int"))
          }
          catch {
            case _: NumberFormatException => tempdf = tempdf.withColumn(cl.name, col(cl.name).cast("double"))
          }
        case _ =>
      }
    })
    tempdf
  }

  /**Gets the data types of the columns
   * @param head A single row from the DataFrame
   * @param columns The DataFrame's columns
   * @return An object storing the column's name and its data type
   */
  private def dataTypeFind(head: Row, columns: Array[String]): Array[ColumnType] = {

    val values = new Array[ColumnType](columns.length)
    for (x <- 0 until head.length) {
      val col = columns(x)
      if (dateTimeCheck(head.getString(x)))
        values(x) = ColumnType(col, "datetime")
      else if (dateCheck(head.getString(x)))
        values(x) = ColumnType(col, "date")
      else if (intCheck(head.getString(x)))
        values(x) = ColumnType(col, "int")
      else if (doubleCheck(head.getString(x)))
        values(x) = ColumnType(col, "double")
      else
        values(x) = ColumnType(col, "string")
    }
    values
  }

  /**Checks if the column is an integer type
   * @param str The string value of the column
   * @return True if its an integer or false otherwise
   */
  private def intCheck(str: String): Boolean = {
    if ("^\\d{1,10}$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }

  /**Checks if the column is a double type
   * @param str The string value of the column
   * @return True if its a double or false otherwise
   */
  private def doubleCheck(str: String): Boolean = {
    if ("^\\d+\\.\\d+$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }

  /**Checks if the column is a dateTime type
   * @param str The string value of the column
   * @return True if its a dateTime or false otherwise
   */
  private def dateTimeCheck(str: String): Boolean = {
    if ("^\\d{1,4}/\\d{1,4}/\\d{1,4} \\d\\d:\\d\\d$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }

  /**Checks if the column is a date type
   * @param str The string value of the column
   * @return True if its a date or false otherwise
   */
  private def dateCheck(str: String): Boolean = {
    if ("(\\d|\\d{2})([/-])(\\d|\\d{2})([/-])\\d{4}".r.findFirstMatchIn(str).nonEmpty) true
    else if ("\\d{4}([/-])(\\d|\\d{2})([/-])(\\d|\\d{2})".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }

  /**Case class for storing the column's name and data type
   * @param name The column's name
   * @param collType The column's data type
   */
  private case class ColumnType(name: String, collType: String) {
    def this() {
      this("", "")
    }
  }
}
