package bls

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Subs_pp_balances(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  //var v_date = propMap("v_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  val bls_balances_path = "/dmp/daily_stg/bls/stg_subs_balances_daily/"
  val subs_pp_path = "/dmp/daily_stg/bls/subs_pp_info/"
  var status = true

  def RunJob():Boolean= {
    if (spark != null)
      try {
      tools.removeFolder(bls_balances_path)
      tools.removeFolder(subs_pp_path)
        // stg_subs_balances_daily
        val csa = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/comverse_sub_archive/"+
          tools.patternToDate(P_DATE,"MM-yyyy")+"/"+
          tools.patternToDate(tools.addDays(P_DATE,-2),"ddMMyyyy")+".parq").createOrReplaceTempView("comverse")

        spark.sql("select * from comverse").write.parquet(bls_balances_path+tools.patternToDate(P_DATE,"ddMMyyyy"))

        // price_plan_subs
        val sa = spark.read.parquet("/mnt/gluster-storage/etl/download/karkaz_bc/service_agreement/")
        val soc = spark.read.parquet("/mnt/gluster-storage/etl/download/karkaz_bc/soc/")
        val dpp = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_price_plan/")

        sa.filter($"SERVICE_TYPE"==="P" && $"EFFECTIVE_DATE".cast(org.apache.spark.sql.types.DateType)<date_add(current_date(),1))
          .join(soc,sa("soc")===soc("soc"),"left")
          .join(dpp,trim(soc("soc"))===trim(dpp("PRICE_PLAN_KEY")),"left")
          .select(
            sa("SUBSCRIBER_NO").as("SUBS_KEY"),
            sa("BAN").as("BAN_KEY"),
            sa("SOC"),
            soc("SOC_DESCRIPTION"),
            dpp("PP_GROUP"),
            sa("SYS_CREATION_DATE"),
            sa("EFFECTIVE_DATE"),
            row_number().over(Window.partitionBy(sa("SUBSCRIBER_NO"),sa("BAN")).orderBy(sa("EFFECTIVE_DATE").desc,sa("SYS_CREATION_DATE").desc)).as("RN")
          )
          .filter($"RN".isin(1,2))
          .groupBy($"SUBS_KEY",$"BAN_KEY")
          .agg(
            lit(0).as("ARCH_PP_IND"),
            max(when($"RN" === 1, $"SOC").otherwise(null)).as("CURR_PRICE_PLAN_CODE"),
            max(when($"RN" === 1, $"PP_GROUP").otherwise(null)).as("PP_GROUP"),
            max(when($"RN" === 1, $"EFFECTIVE_DATE").otherwise(null)).as("PRICE_PLAN_CHANGE_DATE"),
            max(when($"RN" === 2, $"SOC").otherwise(null)).as("PREV_PRICE_PLAN_CODE")
          ).write.parquet(subs_pp_path+tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}