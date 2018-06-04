package utl

import java.io.File
import java.time.format.DateTimeFormatter
import java.time._
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.Column

import scala.collection.mutable.ListBuffer

class Tools(ZoneID: String, dtShortFormat: String, dtFullFormat: String, propMap: Map[String, String]) {

  def initFSConfiguration(propMap: Map[String, String]):Configuration= {
    val conf = new Configuration()
    conf.set("fs.igfs.impl", propMap("fs.igfs.impl"))
    conf.set("fs.hdfs.impl", propMap("fs.hdfs.impl"))
    conf.set("fs.file.impl", propMap("fs.file.impl"))
    conf.set("fs.defaultFS", propMap("fs.defaultFS"))
    conf
  }

  def removeFolder(folder: String):Boolean={
    val conf = new Configuration()
    var removeStatus = false
    conf.set("fs.igfs.impl", propMap("fs.igfs.impl"))
    conf.set("fs.hdfs.impl", propMap("fs.hdfs.impl"))
    conf.set("fs.file.impl", propMap("fs.file.impl"))
    conf.set("fs.defaultFS", propMap("fs.defaultFS")) // Active
    val fs = FileSystem.get(conf)
    if (fs.exists(new org.apache.hadoop.fs.Path(folder))) {
      removeStatus = fs.delete(new org.apache.hadoop.fs.Path(folder), true)
    }
    removeStatus
  }

  def createFolder(folder: String):Boolean={
    val conf = new Configuration()
    var createStatus = true
    conf.set("fs.igfs.impl", propMap("fs.igfs.impl"))
    conf.set("fs.hdfs.impl", propMap("fs.hdfs.impl"))
    conf.set("fs.file.impl", propMap("fs.file.impl"))
    conf.set("fs.defaultFS", propMap("fs.defaultFS")) // Active
    val fs = FileSystem.get(conf)
    if (!fs.exists(new org.apache.hadoop.fs.Path(folder)))
      createStatus = fs.mkdirs(new org.apache.hadoop.fs.Path(folder))
    createStatus
  }

  def checkExistFolder(folder: String):Boolean={
    val fs = FileSystem.get(initFSConfiguration(propMap))
    fs.exists(new org.apache.hadoop.fs.Path(folder))
  }

  def getListOfFiles(dir: String, mask: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(_.isFile).filter(_.getPath.endsWith(mask)).toList
    else List[File]()
  }

  def dateDiff(dtStr1:String, dtStr2:String):Long =
    Math.abs(LocalDate.parse(dtStr1).until(LocalDate.parse(dtStr2),ChronoUnit.DAYS))

  def patternToDate(dtStr: String, pattern: String): String =
    LocalDate.parse(dtStr).format(DateTimeFormatter.ofPattern(pattern))

  def targetFolderByDate(dtStr: String, pattern: String): String =
    LocalDate.parse(dtStr).format(DateTimeFormatter.ofPattern(pattern))

  def strTail = udf((str: String) => str.substring(2))

  def getSysDate(fmt: String): String =
    LocalDateTime.now(ZoneId.of(ZoneID)).format(DateTimeFormatter.ofPattern(fmt))

  def addMonths(dtStr: String, mn:Int): String =
    LocalDate.parse(dtStr).plusMonths(mn).format(DateTimeFormatter.ofPattern(dtShortFormat))

  def addDays(dtStr: String, mn: Int): String =
    LocalDate.parse(dtStr).plusDays(mn).format(DateTimeFormatter.ofPattern(dtShortFormat))

  def truncDate(dtStr: String, fmt: String): String =
    if (fmt.equalsIgnoreCase("mm") || fmt.equalsIgnoreCase("month") || fmt.equalsIgnoreCase("mon"))
      LocalDate.parse(dtStr).withDayOfMonth(1).format(DateTimeFormatter.ofPattern(dtShortFormat))
    else dtStr

  def getStartOfDay(dtStr: String): String={
    ZonedDateTime.of(LocalDate.parse(dtStr), LocalTime.of(0,0),ZoneId.of(ZoneID))
      .format(DateTimeFormatter.ofPattern(dtFullFormat))
  }

  def getEndOfDay(dtStr: String): String=
    ZonedDateTime.of(LocalDate.parse(dtStr), LocalTime.of(23,59,59),ZoneId.of(ZoneID))
      .format(DateTimeFormatter.ofPattern(dtFullFormat))

  def lastDay(dtStr: String): String =
    LocalDate.parse(dtStr).withDayOfMonth(LocalDate.parse(dtStr).lengthOfMonth())
      .format(DateTimeFormatter.ofPattern(dtShortFormat))

  def firstMonthDay(dtStr: String, pattern: String): String =
    LocalDate.parse(dtStr).withDayOfMonth(1).format(DateTimeFormatter.ofPattern(pattern))

  def getDateFromXml(s:String):String = {
    s match {
      case "sysdate" => getSysDate("yyyy-MM-dd")
      case "sysmonth" => firstMonthDay(addMonths(getSysDate("yyyy-MM-dd"),-1),"yyyy-MM-dd")
      case s => s
    }
  }

  def initNotification(msgBody:List[String], caseName: String, notifType: String, fs: FileSystem):Unit = {
    println(caseName + " " + notifType + " notification file is creating...")
    // Make message file
    val outStr = new StringBuilder()
    outStr.append(notifType.toUpperCase +" notification file. Creating time: "+getSysDate("yyyy-MM-dd HH:mm:ss")+"\n")
    outStr.append(caseName.toLowerCase + " calculation log. Please check information below."+"\n")
    outStr.append("------------------------------------"+"\n")
    msgBody foreach { x => outStr.append(x+"\n") }
    // **************************
    try {
      if (!propMap("nfolder").isEmpty && fs.exists(new Path(propMap("nfolder")))) {
        val fn = propMap("nfolder") + "/" + getSysDate("yyyyMMdd") + "_" + caseName.toLowerCase + "_" + notifType.toLowerCase + ".log"
        val nf = fs.create(new Path(fn), true)
        nf.writeBytes(outStr.toString())
        nf.close()
        outStr.clear()
      }
      else println("Cannot create file")
    }catch { case e: Exception => e.printStackTrace() }
  }

  def checkHiveTable(db:String, tbl:String):String = s"show tables in $db like '$tbl'"

  def addPartitionToHive(db:String, tbl:String, field:String, pattern:String, location:String):String =
    s"alter table $db.$tbl add partition ($field='$pattern') location '$location'"

  def dropPartitionToHive(db:String, tbl:String, field:String, pattern:String):String =
    s"alter table $db.$tbl drop if exists partition ($field='$pattern')"

  def createParquetHiveTbl(fields:List[String], db:String, tbl:String, pfield:String, location:String):String = {
    val tblRow = new ListBuffer[String]()
    tblRow.append(s"create external table $db.$tbl"+"\n")
    tblRow.append("("+"\n")
    var i = 0
    while(i<fields.size){
      if(i<fields.size-1) tblRow.append(fields(i)+","+"\n")
      else tblRow.append(fields(i)+"\n")
      i += 1
    }
    tblRow.append(")"+"\n")
    tblRow.append(s"partitioned by ($pfield)"+"\n")
    tblRow.append("stored as parquet"+"\n")
    tblRow.append(s"location '$location'")
    tblRow.toList.mkString
  }

  def createOrcHiveTbl(fields:List[String], db:String, tbl:String, pfield:String, location:String):String = {
    val tblRow = new ListBuffer[String]()
    tblRow.append(s"create external table $db.$tbl"+"\n")
    tblRow.append("("+"\n")
    var i = 0
    while(i<fields.size){
      if(i<fields.size-1) tblRow.append(fields(i)+","+"\n")
      else tblRow.append(fields(i)+"\n")
      i += 1
    }
    tblRow.append(")"+"\n")
    tblRow.append(s"partitioned by ($pfield)"+"\n")
    tblRow.append("stored as orc"+"\n")
    tblRow.append(s"location '$location'")
    tblRow.toList.mkString
  }



}
