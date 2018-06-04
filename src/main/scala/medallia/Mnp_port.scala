package medallia

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Mnp_port(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val mnp_path = "/dmp/daily_stg/medallia/mnp/"

  def RunJob():Boolean= {
    if (spark != null)
      try {

        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/mnp_process/*/").createOrReplaceTempView("mnp_process")
        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/mnp_process_statuses/").createOrReplaceTempView("mnp_process_statuses")
        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/operators/").createOrReplaceTempView("operators")
        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/mnp_process_mb/*/").createOrReplaceTempView("mnp_process_mb")
        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/mnp_process_type/").createOrReplaceTempView("mnp_process_type")
        spark.read.parquet("/mnt/gluster-storage/etl/download/mnp/dict_data/").createOrReplaceTempView("dict_data")

        val df1 = spark.sql(s"""
        SELECT
        I.FD,
        A.ID,
        O.CODE AS DONOR_NAME,
        Q.CODE AS RECIPIENT_NAME,
        B.UNIQ,
        S.STATUS_NAME,
        I.MOB_NUM,
        I.BAN,
        I.MARKET_ID,
        I.ACCOUNT_TYPE_ID,
        I.TMP_MOB_NUM,
        CASE WHEN A.DONOR_ID = 1 THEN 'PORT OUT'
        WHEN A.RECIPIENT_ID = 1 THEN 'PORT IN' END AS PORTATION_TYPE,
        A.DEALER_POINT_ID,
        A2.USR AS USER_ID
        FROM MNP_PROCESS A
        LEFT JOIN MNP_PROCESS A2
        ON A.ID = A2.ID
        AND A2.STATUS = 101,
        MNP_PROCESS_STATUSES S,
        OPERATORS O,
        OPERATORS Q,
        MNP_PROCESS_MB I,
        MNP_PROCESS_TYPE B,
        (SELECT * FROM DICT_DATA WHERE UP = 21) DD2
        WHERE 1 = 1
        AND O.ID = A.DONOR_ID
        AND O.TD >= current_date()
        AND Q.ID = A.RECIPIENT_ID
        AND Q.TD >= current_date()
        AND  A.PROCESS_TYPE_ID = B.ID
        AND B.ID IN (2,5)
        AND A.ID = I.PROCESS_ID
        AND A.STATUS = S.STATUS
        AND S.IS_FINAL = 1
        AND S.STATUS_TYPE = 1
        AND S.STATUS = 1
        AND A.PROCESS_TYPE_ID = S.PROCESS_TYPE_ID
        AND I.FD >= current_timestamp() + interval -25 days
        AND I.FD <  current_timestamp() + interval -20 days + interval 10 minutes
        AND DD2.CODE = I.STATUS
        AND DD2.CODE = 6
        AND A.RECIPIENT_ID = 1
        """
        ).write.parquet(mnp_path + tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally {}
    status
  }
}