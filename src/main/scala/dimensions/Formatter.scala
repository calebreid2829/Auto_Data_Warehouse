package dimensions

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}

protected object Formatter {
  def format(df:DataFrame):DataFrame={
    val format = (str:String) =>{
      if("(\\d|\\d{2})([/-])(\\d|\\d{2})([/-])\\d{4}".r.findFirstMatchIn(str).nonEmpty)
        if("/".r.findFirstMatchIn(str).nonEmpty) "MM/DD/YYYY"
        else "MM-DD-YYYY"
      else if("\\d{4}([/-])(\\d|\\d{2})([/-])(\\d|\\d{2})".r.findFirstMatchIn(str).nonEmpty)
        if("/".r.findFirstMatchIn(str).nonEmpty) "YYYY/MM/DD"
        else "YYYY-MM-DD"
    }
    var tempdf = df
    val dataTypes = dataTypeFind(tempdf.head,tempdf.columns)
    dataTypes.foreach(cl =>{
      cl.collType match{
        case "datetime" => tempdf = tempdf.withColumn(cl.name, to_timestamp(col(cl.name), "MM/dd/yyyy HH:mm"))
        case "date" => tempdf = tempdf.withColumn(cl.name, to_date(col(cl.name), "MM/dd/yyyy"))
        case "int" => tempdf = tempdf.withColumn(cl.name,col(cl.name).cast("int"))
        case "double" =>
          try{
            tempdf = tempdf.withColumn(cl.name,col(cl.name).cast("int"))
          }
          catch{
            case _: NumberFormatException=> tempdf = tempdf.withColumn(cl.name,col(cl.name).cast("double"))
          }
        case _=>
      }
    })
    tempdf
  }
  private def dataTypeFind(head:Row,columns: Array[String]): Array[CollumnType]={

    val values = new Array[CollumnType](columns.length)
    for(x<- 0 until head.length){
      val col = columns(x)
      if(dateTimeCheck(head.getString(x)))
        values(x) = CollumnType(col,"datetime")
      else if(dateCheck(head.getString(x)))
        values(x) = CollumnType(col,"date")
      else if(intCheck(head.getString(x)))
        values(x) = CollumnType(col,"int")
      else if(doubleCheck(head.getString(x)))
        values(x) = CollumnType(col,"double")
      else
        values(x) = CollumnType(col,"string")
    }
    values
  }
  private def intCheck(str:String): Boolean={
    if("^\\d{1,10}$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }
  private def doubleCheck(str:String): Boolean={
    if("^\\d+\\.\\d+$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }
  private def dateTimeCheck(str:String): Boolean={
    if("^\\d{1,4}/\\d{1,4}/\\d{1,4} \\d\\d:\\d\\d$".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }
  private def dateCheck(str:String): Boolean={
    if("(\\d|\\d{2})([/-])(\\d|\\d{2})([/-])\\d{4}".r.findFirstMatchIn(str).nonEmpty) true
    else if("\\d{4}([/-])(\\d|\\d{2})([/-])(\\d|\\d{2})".r.findFirstMatchIn(str).nonEmpty) true
    else false
  }

  private case class CollumnType(name:String,collType:String){
    def this(){
      this("","")
    }
  }
}
