package bls

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Prior_region(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  //var v_date = propMap("v_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  val lcs_path = "/dmp/daily_stg/bls/lac_cell_site_actual"
  var status = true

  def RunJob():Boolean= {
    if (spark != null)
      try {
        val df = spark.read.parquet("/dmp/daily_stg/tg_subs_lc_hh/*/")
        val df1 = spark.read.parquet(lcs_path)
        // monthly
        df.filter(
          $"period".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-30) &&
            $"period".cast(org.apache.spark.sql.types.DateType)<P_DATE).join(
          df1,df("lac")===df1("lac") &&
            df("cell_id")===df1("cell_id"),"left")
          .groupBy(
          lit(P_DATE).as("CALC_DATE"),
          df("subs_key"),
          df("ban_key"),
          df1("region")).agg(
          countDistinct(df("period")).as("uniq_days_amt"),
          sum(df("call_out_amt")).as("call_out_amt"),
          sum(df("call_in_amt")).as("call_in_amt"),
          sum(df("call_in_duration_min")).as("call_in_duration_min"),
          sum(df("call_out_duration_min")).as("call_out_duration_min"),
          sum(df("data_vlm_mb")).as("data_vlm_mb"),
          sum(when(df1("cell_nettype")==="2G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
            when(df1("cell_nettype")==="3G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*21.4)/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
              when(df1("cell_nettype")==="4G",df("data_vlm_mb")).otherwise(
                when(df1("cell_nettype").isNull,((((df("call_in_duration_min")+df("call_out_duration_min")) *60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb"))
              )
            )
          )).as("voice_gprs_mrg_mb"))
          .write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_monthly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val sd = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_monthly/*/")

        sd.filter($"region".isNotNull && $"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE).select(
          lit(P_DATE).as("calc_date"),
          $"subs_key",
          $"ban_key",
          $"region",
          row_number().over(Window.partitionBy($"calc_date",$"subs_key",$"ban_key")
            .orderBy($"uniq_days_amt".desc,$"voice_gprs_mrg_mb".desc,$"call_out_amt".desc,$"call_in_amt".desc)).as("rn")
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_monthly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val st = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_monthly/*/")

        st.filter($"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE && $"rn"===1).select(
          lit(P_DATE).as("period"),
          $"subs_key",
          $"ban_key",
          $"region"
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_prior_region_monthly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        // weekly
        df.filter(
          $"period".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-7) &&
            $"period".cast(org.apache.spark.sql.types.DateType)<P_DATE).join(
          df1,df("lac")===df1("lac") &&
            df("cell_id")===df1("cell_id"),"left")
          .groupBy(
            lit(P_DATE).as("CALC_DATE"),
            df("subs_key"),
            df("ban_key"),
            df1("region")).agg(
          countDistinct(df("period")).as("uniq_days_amt"),
          sum(df("call_out_amt")).as("call_out_amt"),
          sum(df("call_in_amt")).as("call_in_amt"),
          sum(df("call_in_duration_min")).as("call_in_duration_min"),
          sum(df("call_out_duration_min")).as("call_out_duration_min"),
          sum(df("data_vlm_mb")).as("data_vlm_mb"),
          sum(when(df1("cell_nettype")==="2G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
            when(df1("cell_nettype")==="3G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*21.4)/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
              when(df1("cell_nettype")==="4G",df("data_vlm_mb")).otherwise(
                when(df1("cell_nettype").isNull,((((df("call_in_duration_min")+df("call_out_duration_min")) *60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb"))
              )
            )
          )).as("voice_gprs_mrg_mb"))
          .write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_weekly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val sd1 = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_weekly/*/")

        sd1.filter($"region".isNotNull && $"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE).select(
          lit(P_DATE).as("calc_date"),
          $"subs_key",
          $"ban_key",
          $"region",
          row_number().over(Window.partitionBy($"calc_date",$"subs_key",$"ban_key")
            .orderBy($"uniq_days_amt".desc,$"voice_gprs_mrg_mb".desc,$"call_out_amt".desc,$"call_in_amt".desc)).as("rn")
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_weekly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val st1 = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_weekly/*/")

        st1.filter($"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE && $"rn"===1).select(
          lit(P_DATE).as("period"),
          $"subs_key",
          $"ban_key",
          $"region"
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_prior_region_weekly/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        // 3 months
        df.filter(
          $"period".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-90) &&
            $"period".cast(org.apache.spark.sql.types.DateType)<P_DATE).join(
          df1,df("lac")===df1("lac") &&
            df("cell_id")===df1("cell_id"),"left")
          .groupBy(
            lit(P_DATE).as("CALC_DATE"),
            df("subs_key"),
            df("ban_key"),
            df1("region")).agg(
          countDistinct(df("period")).as("uniq_days_amt"),
          sum(df("call_out_amt")).as("call_out_amt"),
          sum(df("call_in_amt")).as("call_in_amt"),
          sum(df("call_in_duration_min")).as("call_in_duration_min"),
          sum(df("call_out_duration_min")).as("call_out_duration_min"),
          sum(df("data_vlm_mb")).as("data_vlm_mb"),
          sum(when(df1("cell_nettype")==="2G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
            when(df1("cell_nettype")==="3G",((((df("call_in_duration_min")+df("call_out_duration_min"))*60*21.4)/(8*(1024*1024)))*1024)+df("data_vlm_mb")).otherwise(
              when(df1("cell_nettype")==="4G",df("data_vlm_mb")).otherwise(
                when(df1("cell_nettype").isNull,((((df("call_in_duration_min")+df("call_out_duration_min")) *60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("data_vlm_mb"))
              )
            )
          )).as("voice_gprs_mrg_mb"))
          .write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_3month/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val sd2 = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_region_3month/*/")

        sd2.filter($"region".isNotNull && $"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE).select(
          lit(P_DATE).as("calc_date"),
          $"subs_key",
          $"ban_key",
          $"region",
          row_number().over(Window.partitionBy($"calc_date",$"subs_key",$"ban_key").orderBy($"uniq_days_amt".desc,$"voice_gprs_mrg_mb".desc,$"call_out_amt".desc,$"call_in_amt".desc)).as("rn")
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_3month/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        val st2 = spark.read.parquet("/dmp/daily_stg/stg_cvm_subs_loc_prior_region_3month/*/")

        st2.filter($"calc_date".cast(org.apache.spark.sql.types.DateType)===P_DATE && $"rn"===1).select(
          lit(P_DATE).as("period"),
          $"subs_key",
          $"ban_key",
          $"region"
        ).write.parquet("/dmp/daily_stg/stg_cvm_subs_prior_region_3month/"+tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}