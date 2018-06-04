package bls

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import utl.SparkBase
import utl.Tools

class Stg_subs_lc_hh(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(tools.addDays(P_DATE,-3),"ddMMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  val n = -3 // Actual
  //val stg_tbl = "tg_subs_lc_hh"
  import spark.implicits._

  def get_ds_path(source:String, nm: String, ext: String, n:Integer):String =
    propMap("dsfolder") + "/" + source + "/" + nm + "/" + tools.patternToDate(tools.addDays(P_DATE,n),"MM-yyyy") + "/" + ext
  def get_ref_ds_path(source:String, nm: String, ext: String):String =
    propMap("dsfolder") + "/" + source + "/" + nm + "/" + ext

  val df_path = sFolder + "/" + "tg_subs_lc_hh" + "/*/"
  val df_month_path = sFolder + "/" + "tmp" + "/" + "tg_subs_lc_hh_m"
  val df1_path = sFolder + "/bls/lac_cell_site_actual"
  val df2_path = "/dmp/upload/cvm/dim_calendar"
  val subs_loc_region_mon_f_path = sFolder + "/" + "subs_loc_region_mon_f" + "/"
  val subs_loc_prior_region_mon_f_path = sFolder + "/" + "subs_loc_prior_region_mon_f" + "/"
  val subs_loc_prior_region_f_path = sFolder + "/" + "subs_loc_prior_region_f" + "/"

  val tg_det_layer_parquet_path = sFolder + "/" + "tg_det_layer_trans_parq" + "/" + tools.patternToDate(tools.addDays(P_DATE,n), "ddMMyyyy")

  def RunJob():Boolean = {
    var status = RunTask_1(sFolder + "/" + "tg_subs_lc_hh" + "/" + tools.patternToDate(tools.addDays(P_DATE,n),"ddMMyyyy")) // tg_subs_lc_hh
    if (status) status = RunTask_2() // subs_loc_region_mon_f_path, subs_loc_prior_region_mon_f, subs_loc_prior_region_f
    status
  }

  def RunTask_1(stg_path:String):Boolean = {
    var status = true
    if (spark != null)
      try {
        // Check staging folders and remove it
        tools.removeFolder(stg_path)
        // Script initialization
        val tg_det_layer_parquet = spark.read.parquet(tg_det_layer_parquet_path)
        val tg_subs_lc_hh =
          tg_det_layer_parquet
            .filter(
              $"COUNTED_CDR_IND".===("1").||($"CHARGE_AMT".=!=(0)) &&
                $"CALL_FORWARD_IND".=!=(1) &&
                $"CALL_TYPE_CODE".isin("V","G") &&
                ($"ROAMING_IND".===("0").&&($"ROAMING_TYPE_KEY".isin("H","X")) || $"ROAMING_IND".===("1").&&($"ROAMING_TYPE_KEY".===("R"))))
            .groupBy(
              date_format($"CALL_START_TIME","yyyy-MM-dd").as("PERIOD"),
              date_format($"CALL_START_TIME","HH").as("HHOUR_ID"),
              $"SUBS_KEY",
              $"BAN_KEY",
              $"LAC".cast("Long").cast(DataTypes.createDecimalType(30,0)).as("LAC"),
              $"CELL_ID".cast("Long").cast(DataTypes.createDecimalType(30,0)).as("CELL_ID")
            )
            .agg(
              sum(when($"CALL_TYPE_CODE".===("V").&&($"CALL_DIRECTION_IND".===("2")),1).otherwise(0)).cast(DataTypes.createDecimalType(30,0)).as("CALL_OUT_AMT"),
              sum(when($"CALL_TYPE_CODE".===("V").&&($"CALL_DIRECTION_IND".===("1")),1).otherwise(0)).cast(DataTypes.createDecimalType(30,0)).as("CALL_IN_AMT"),
              sum(when($"CALL_TYPE_CODE".===("V").&&($"CALL_DIRECTION_IND".===("2")),$"ACTUAL_CALL_DURATION_SEC"./(60)).otherwise(0)).as("CALL_OUT_DURATION_MIN"),
              sum(when($"CALL_TYPE_CODE".===("V").&&($"CALL_DIRECTION_IND".===("1")),$"ACTUAL_CALL_DURATION_SEC"./(60)).otherwise(0)).as("CALL_IN_DURATION_MIN"),
              sum(when($"CALL_TYPE_CODE".===("G"),$"ROUNDED_DATA_VOLUME"./(1024)./(1024)).otherwise(0)).as("DATA_VLM_MB")
            )
            .cache()
        tg_subs_lc_hh.write.parquet(stg_path)
        // Hive add partition
        /* val tbl = if (!hiveMap("db").isEmpty) hiveMap("db")+"."+stg_tbl else stg_tbl
        val q1 = "alter table "+ tbl + " drop if exists partition (dt='" + tools.patternToDate(tools.addDays(P_DATE,n),"ddMMyyyy") + "')"
        val q2 = "alter table "+ tbl + " add partition (dt='" + tools.patternToDate(tools.addDays(P_DATE,n),"ddMMyyyy") + "')" +
          " location '" + stg_path + "'"
        spark.sql(q1)
        tg_subs_lc_hh.write.parquet(stg_path)
        spark.sql(q2) */
      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally { }
    else status = false
    status
  }

  def RunTask_2():Boolean = {
    var status = true
    if (spark != null)
      try {

        // Check staging folders and remove it
        tools.removeFolder(subs_loc_region_mon_f_path)
        tools.removeFolder(subs_loc_prior_region_mon_f_path)
        tools.removeFolder(subs_loc_prior_region_f_path)
        tools.removeFolder(df_month_path)
        spark.read
          .parquet(df_path)
          .filter($"PERIOD".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-30) &&
            $"PERIOD".cast(org.apache.spark.sql.types.DateType)<P_DATE)
          .write.parquet(df_month_path)
        val df = spark.read.parquet(df_month_path)
        // **************************************
        val df1 = spark.read.parquet(df1_path)
        val df2 = spark.read.parquet(df2_path)
        // Script initialization
        df
          //.filter(
          //$"PERIOD".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-30) &&
          //  $"PERIOD".cast(org.apache.spark.sql.types.DateType)<P_DATE)
          .join(df1,df("LAC")===df1("LAC") && df("CELL_ID")===df1("CELL_ID"),"left")
          .groupBy(
            lit(P_DATE).as("CALC_DATE"),
            df("SUBS_KEY"),
            df("BAN_KEY"),
            df1("REGION"))
          .agg(
            countDistinct(df("PERIOD").cast(DataTypes.createDecimalType(30,0))).as("UNIQ_DAYS_AMT"),
            sum(df("CALL_OUT_AMT")).as("CALL_OUT_AMT"),
            sum(df("CALL_IN_AMT")).as("CALL_IN_AMT"),
            sum(df("CALL_IN_DURATION_MIN")).as("CALL_IN_DURATION_MIN"),
            sum(df("CALL_OUT_DURATION_MIN")).as("CALL_OUT_DURATION_MIN"),
            sum(df("DATA_VLM_MB")).as("DATA_VLM_MB"),
            sum(when(df1("cell_nettype")==="2G",((((df("CALL_IN_DURATION_MIN")+df("CALL_OUT_DURATION_MIN"))*60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("DATA_VLM_MB")).otherwise(
              when(df1("cell_nettype")==="3G",((((df("CALL_IN_DURATION_MIN")+df("CALL_OUT_DURATION_MIN"))*60*21.4)/(8*(1024*1024)))*1024)+df("DATA_VLM_MB")).otherwise(
                when(df1("cell_nettype")==="4G",df("DATA_VLM_MB")).otherwise(
                  when(df1("cell_nettype").isNull,((((df("CALL_IN_DURATION_MIN")+df("CALL_OUT_DURATION_MIN")) *60*(2*(16*0.25+8*0.75)))/(8*(1024*1024)))*1024)+df("DATA_VLM_MB"))
                )
              )
            )).as("VOICE_GPRS_MRG_MB"))
          .write.parquet(subs_loc_region_mon_f_path)
        // *************************************
        val subs_loc_region_mon_f = spark.read.parquet(subs_loc_region_mon_f_path)
        subs_loc_region_mon_f
          .filter($"REGION".isNotNull)
          .select(
            lit(P_DATE).as("CALC_DATE"),
            $"SUBS_KEY",
            $"BAN_KEY",
            $"REGION",
            row_number().over(Window.partitionBy($"CALC_DATE",$"SUBS_KEY",$"BAN_KEY")
              .orderBy($"UNIQ_DAYS_AMT".desc,$"VOICE_GPRS_MRG_MB".desc,$"CALL_OUT_AMT".desc,$"CALL_IN_AMT".desc)).as("RN")
          )
          .write.parquet(subs_loc_prior_region_mon_f_path)
        // *************************************
        val subs_loc_prior_region_mon_f = spark.read.parquet(subs_loc_prior_region_mon_f_path)
        subs_loc_prior_region_mon_f
          .filter($"CALC_DATE".cast(org.apache.spark.sql.types.DateType)===P_DATE && $"RN"===1)
          .select(
            lit(P_DATE).as("CALC_DATE"),
            $"SUBS_KEY",
            $"BAN_KEY",
            $"REGION"
          ).write.parquet(subs_loc_prior_region_f_path)

        spark.close()
      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally {}
    else status = false
    status
  }
}