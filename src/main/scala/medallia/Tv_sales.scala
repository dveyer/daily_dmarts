package medallia

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Tv_sales(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val tv_path = "/dmp/daily_stg/medallia/tv/"

  def RunJob():Boolean= {
    if (spark != null)
      try {

        spark.read.parquet("/mnt/gluster-storage/etl/download/suz/fmc_order_data/*/").createOrReplaceTempView("fmc_order_data")
        spark.read.parquet("/mnt/gluster-storage/etl/download/suz/fmc_change_status_log/*/").createOrReplaceTempView("fmc_change_status_log")

        val df1 = spark.sql(s"""
         select
         q.id,
         q.order_status_id,
         q.order_status_date,
         q.login_fttb,
         q.ban,
         q.fttb_subscriber_type,
         q.fmc_type_id,
         q.phone1,
         q.phone2,
         q.phone3,
         q.user_id,
         q.sl_dealer_cd,
         q.sl_dealer_name,
         q.mon_dealer_cd,
         q.prev_req_id,
         q.prev_req_status,
         q.prev_fmc_req_type,
         from
         (select
         t.*,
         b.id as prev_req_id,
         b.order_status_id as prev_req_status,
         b.fmc_type_id as prev_fmc_req_type,
         row_number () over (partition by t.id,t.ban,t.login_fttb,t.order_status_date order by b.id desc) as rn
         from
         (select
         a.id,
         a.order_status_id,
         a.order_status_date,
         a.login_fttb,
         a.ban,
         a.fttb_subscriber_type,
         a.fmc_type_id,
         a.phone1,
         a.phone2,
         a.phone3,
         a.user_id,
         a.sl_dealer_cd,
         a.sl_dealer_name,
         a.mon_dealer_cd
         from fmc_order_data a
         where a.order_status_id  in (408,9) and a.pre_order_status_id is null
         and a.fmc_type_id in ('3')
         and a.order_status_date >= '2018-05-28'
         and a.order_status_date < '2018-05-29'
         and a.login_fttb is not null
         union all
         select
         b.fmc_order_id id,
         b.pre_order_status_id as order_status_id,
         b.pre_order_status_date as order_status_date,
         a.login_fttb,
         a.ban,
         a.fttb_subscriber_type,
         a.fmc_type_id,
         a.phone1,
         a.phone2,
         a.phone3,
         a.user_id,
         a.sl_dealer_cd,
         a.sl_dealer_name,
         a.mon_dealer_cd
         from fmc_order_data a
         inner join fmc_change_status_log b
         on a.id = b.fmc_order_id
         and b.pre_order_status_id in (408,9)
         and b.pre_order_status_date >= '2018-05-28'
         and b.pre_order_status_date < '2018-05-29'
         where a.fmc_type_id in ('3')
         and a.login_fttb is not null)
         t
         left join fmc_order_data b
         on t.login_fttb = b.login_fttb
         and t.id > b.id
         where t.order_status_date >= '2018-05-28 16:00'
         and t.order_status_date < '2018-05-28 17:00' ) q
         where q.rn= 1
         and ((q.fmc_type_id in ('3') and q.prev_fmc_req_type is null)
         or (q.fmc_type_id = '4' and (q.prev_fmc_req_type is null or q.prev_fmc_req_type in ('2')))
         or (q.fmc_type_id = q.prev_fmc_req_type and q.prev_req_status = 4))

        """
        ).write.parquet(tv_path + "28052018") //tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}