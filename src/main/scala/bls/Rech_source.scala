package bls

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Rech_source(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  //var v_date = propMap("v_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  val prior_rech_path_m = "/dmp/daily_stg/bls/prior_rech_source_monthly/"
  val prior_rech_path_w = "/dmp/daily_stg/bls/prior_rech_source_weekly/"
  val prior_rech_path_3m = "/dmp/daily_stg/bls/prior_rech_source_3month/"
  var status = true

  def RunJob():Boolean= {
    if (spark != null)
      try {
        val dl = spark.read.parquet("/dmp/daily_stg/tg_det_layer_trans_parq/*/")
        val dmc = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_mtr_classification/")

        // STG_SUBS_RECH_SOURCE_WEEKLY_F
        dl.filter($"SRC"==="RHA" && $"RECHARGE_AMT">0 && $"RECHARGE_SOURCE".isin("E","S","B") &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-6) &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)<tools.addDays(P_DATE,1))
          .join(dmc,dl("MTR_CLASSIFICATION_KEY")===dmc("MTR_CLASSIFICATION_KEY") && dl("PARENT_MTR_KEY")===dmc("PARENT_MTR_KEY") &&
                dmc("TARGET_TABLE_NAME")==="FCT_PREPAID_RECHARGES")
            .groupBy(
              dl("SUBS_KEY"),
              dl("BAN_KEY"),
              dmc("DESCRIPTION").as("RECH_SRC_DESC"))
            .agg(
              sum(dl("RECHARGE_AMT")).as("TOTAL_RECH_AMT"),
              count(lit(1)).as("RECH_COUNT"),
              row_number().over(Window.partitionBy(dl("SUBS_KEY"),dl("BAN_KEY")).orderBy(sum(dl("RECHARGE_AMT")).desc,count(lit(1)).desc)).as("RN")
            ).write.parquet(prior_rech_path_w+tools.patternToDate(P_DATE,"ddMMyyyy"))

        // STG_SUBS_RECH_SOURCE_MONTHLY_F
        dl.filter($"SRC"==="RHA" && $"RECHARGE_AMT">0 && $"RECHARGE_SOURCE".isin("E","S","B") &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-29) &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)<tools.addDays(P_DATE,1))
          .join(dmc,dl("MTR_CLASSIFICATION_KEY")===dmc("MTR_CLASSIFICATION_KEY") && dl("PARENT_MTR_KEY")===dmc("PARENT_MTR_KEY") &&
            dmc("TARGET_TABLE_NAME")==="FCT_PREPAID_RECHARGES")
          .groupBy(
            dl("SUBS_KEY"),
            dl("BAN_KEY"),
            dmc("DESCRIPTION").as("RECH_SRC_DESC"))
          .agg(
            sum(dl("RECHARGE_AMT")).as("TOTAL_RECH_AMT"),
            count(lit(1)).as("RECH_COUNT"),
            row_number().over(Window.partitionBy(dl("SUBS_KEY"),dl("BAN_KEY")).orderBy(sum(dl("RECHARGE_AMT")).desc,count(lit(1)).desc)).as("RN")
          ).write.parquet(prior_rech_path_m+tools.patternToDate(P_DATE,"ddMMyyyy"))

        // STG_SUBS_RECH_SOURCE_3MONTHS_F
        dl.filter($"SRC"==="RHA" && $"RECHARGE_AMT">0 && $"RECHARGE_SOURCE".isin("E","S","B") &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-89) &&
          $"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)<tools.addDays(P_DATE,1))
          .join(dmc,dl("MTR_CLASSIFICATION_KEY")===dmc("MTR_CLASSIFICATION_KEY") && dl("PARENT_MTR_KEY")===dmc("PARENT_MTR_KEY") &&
            dmc("TARGET_TABLE_NAME")==="FCT_PREPAID_RECHARGES")
          .groupBy(
            dl("SUBS_KEY"),
            dl("BAN_KEY"),
            dmc("DESCRIPTION").as("RECH_SRC_DESC"))
          .agg(
            sum(dl("RECHARGE_AMT")).as("TOTAL_RECH_AMT"),
            count(lit(1)).as("RECH_COUNT"),
            row_number().over(Window.partitionBy(dl("SUBS_KEY"),dl("BAN_KEY")).orderBy(sum(dl("RECHARGE_AMT")).desc,count(lit(1)).desc)).as("RN")
          ).write.parquet(prior_rech_path_3m+tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}