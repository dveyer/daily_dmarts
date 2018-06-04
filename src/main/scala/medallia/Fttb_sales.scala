package medallia

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Fttb_sales(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val fttb_path = "/dmp/daily_stg/medallia/fttb/"

  def RunJob():Boolean= {
    if (spark != null)
      try {

        spark.read.parquet("/mnt/gluster-storage/etl/download/suz/suz_order_data").createOrReplaceTempView("suz_order_data")
        spark.read.parquet("/mnt/gluster-storage/etl/download/suz/change_order_status_log").createOrReplaceTempView("change_order_status_log")
        spark.read.parquet("/mnt/gluster-storage/etl/download/cdb/subscriber").createOrReplaceTempView("subscriber")

        val df1 = spark.sql(s"""
          select
          c.id,
          c.login,
          c.ban,
          nvl(trim(s1.subscriber_no),nvl(trim(s2.subscriber_no),trim(s3.subscriber_no))) as phone,
          c.connection_type,
          c.sl_dealer_cd,
          c.sl_dealer_name,
          c.user_id,
          c.order_status_id,
          c.pre_order_status_id,
          case when length(c.phone) > 10 then substr(c.phone,1,10) else phone end as subs_contact_num
          from suz_order_data c
          left join subscriber s1
          on trim(s1.subscriber_no)=trim(c.phone)
          left join subscriber s2
          on trim(s2.subscriber_no)=trim(c.phone2)
          -- and s1.subscriber is null --??
          left join subscriber s3
          on trim(s3.subscriber_no)=trim(c.phone3)
          where c.id in
          (select
          distinct suz_order_data_id
          from
          (select
          a.id as suz_order_data_id,
          a.order_status_id,
          a.order_status_date
          from suz_order_data a
          where a.order_status_id  = 1
          and a.order_status_date >= '2018-05-28'
          and a.order_status_date < '2018-05-29'
          union all
          select
          a.suz_order_data_id,
          a.pre_order_status_id as order_status_id,
          a.pre_order_status_date as order_status_date
          from change_order_status_log a
          where pre_order_status_id = 1
          and a.pre_order_status_date >= '2018-05-28'
          and a.pre_order_status_date < '2018-05-29'
          )
        """
        ).write.parquet(fttb_path + "28052018") //tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}