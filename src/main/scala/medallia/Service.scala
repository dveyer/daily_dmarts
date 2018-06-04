package medallia

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Service(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val service_path = "/dmp/daily_stg/medallia/service/"

  def RunJob():Boolean= {
    if (spark != null)
      try {

        spark.read.parquet("/mnt/gluster-storage/etl/download/karkaz/csm_transactions/*/").createOrReplaceTempView("csm_transactions")
        spark.read.parquet("/mnt/gluster-storage/etl/download/cdb/billing_account").createOrReplaceTempView("billing_account")
        spark.read.parquet("/mnt/gluster-storage/etl/download/cdb/subscriber").createOrReplaceTempView("subscriber")
        spark.read.parquet("/mnt/gluster-storage/etl/download/cdb/service_agreement").createOrReplaceTempView("service_agreement")

        val df1 = spark.sql(s"""
         select
         s.actv_code,
         s.actv_rsn_code,
         s.ban,
         r.subscriber_no,
         case when ba.account_type in ('13','113','37','39') then 'B2C'
              when ba.account_type in ('41') then 'B2B'
              else null end as segment_type,
         s.sys_creation_date,
         s.operator_id,
         concat('ENS_',s.trx_seq_no,'_',s.actv_code) as visit_id,
         case when sa.soc='ICLM_RUS' then 'RUS'
              when sa.soc='ICLM_KAZ' then 'KAZ'
              else null end as language
         from csm_transactions s
         inner join billing_account ba
         on s.ban = ba.ban
         and ba.account_type in ('13','113','37','39','41')
         inner join subscriber r
         on s.ban = r.customer_id
         and r.sub_status = 'A'
         left join
         (select
         distinct sa.ban,
         sa.soc from service_agreement sa
         where sa.service_type = 'O'
         and sa.soc in ('ICLM_RUS','ICLM_KAZ')) sa
         on s.ban = sa.ban
         where s.sys_creation_date >= '2018-05-22 08:00'
         and s.sys_creation_date < '2018-05-22 14:30'
         and (s.actv_code ='MCN' and s.actv_rsn_code in ('ATNO')
         or s.actv_code = 'RPS' and s.actv_rsn_code in ('RPSW')
         or s.actv_code = 'CPP' and s.actv_rsn_code in ('CPPR')
         or s.actv_code = 'CCN' and s.actv_rsn_code in ('CCNW')
         or s.actv_code = 'RSP' and s.actv_rsn_code in ('RSBO', 'RSB')
         or s.actv_code = 'C16'
         or s.actv_code = 'C17'
         or s.actv_code = 'B05'
         or s.actv_code = 'B02'
         or s.actv_code = 'B60'
         or s.actv_code = 'B00'
         or s.actv_code = 'B08'
         or s.actv_code = 'UAE')
         and s.operator_id not like '999%'
        """
        ).write.parquet(service_path + "22052018") //tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}