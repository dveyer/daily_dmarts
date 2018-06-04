package bls

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Subs_profile_ki(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  //var v_date = propMap("v_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val subs_profile_ki_path = "/dmp/daily_stg/bls/subs_profile_ki/"

  def RunJob():Boolean= {
    if (spark != null)
      try {
        /*if (tools.checkExistFolder(subs_profile_ki_path + tools.patternToDate(P_DATE, "ddMMyyyy"))) {
        tools.removeFolder(subs_profile_ki_path + tools.patternToDate(P_DATE, "ddMMyyyy"))
        }*/

        val dbs = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_business_service/").createOrReplaceTempView("dim_business_service")
        val dbst = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_business_service_type/").createOrReplaceTempView("dim_business_service_type")
        val dbcl = spark.read.option("header","true").option("delimiter",";").csv("/dmp/upload/bls/dim_bls_content_list.csv")
          .createOrReplaceTempView("dim_bls_content_list")
        val dgd = spark.read.parquet("/dmp/daily_stg/dim_global_digit").createOrReplaceTempView("dim_global_digit1")
        val dgd1 = spark.read.parquet("/dmp/daily_stg/dim_global_digit").createOrReplaceTempView("dim_global_digit2")
        val dlca = spark.read.parquet("/dmp/daily_stg/bls/lac_cell_site_actual").createOrReplaceTempView("lcs_actual")

          val dl = spark.read.parquet("/dmp/daily_stg/tg_det_layer_trans_parq/" + tools.patternToDate(tools.addDays(P_DATE,-3), "ddMMyyyy"))
            .createOrReplaceTempView("det_layer")
          val df = spark.sql(
            s"""
               SELECT B.SUBS_KEY,
               B.BAN_KEY,
               current_date AS CALC_DATE,
               SUBSTR(B.CALL_START_TIME,1,10) AS PERIOD,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS') OR
               B.SRC = 'CHR' AND B.CHARGE_TYPE IN ('O', 'R')) THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS') OR
               B.SRC = 'CHR' AND B.CHARGE_TYPE IN ('O', 'R')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 42 THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_BUNDLE_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               DBST.REVENUE_STREAM = 1 AND T.BUSINESS_SERVICE_KEY IS NULL THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_VOICE_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               DBST.REVENUE_STREAM = 4 AND T.BUSINESS_SERVICE_KEY IS NULL THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_DATA_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               DBST.REVENUE_STREAM = 3 AND T.BUSINESS_SERVICE_KEY IS NULL THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_SMS_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS') OR
               B.SRC = 'CHR' AND B.CHARGE_TYPE IN ('O', 'R')) AND
               T.BUSINESS_SERVICE_KEY IS NOT NULL THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_CONTENT_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS') OR
               B.SRC = 'CHR' AND B.CHARGE_TYPE IN ('O', 'R')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 9 THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ROAM_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 9 AND
               B.CALL_TYPE_CODE = 'V' THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ROAM_VOICE_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 9 AND
               B.CALL_TYPE_CODE IN ('S', 'M') THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ROAM_SMSMMS_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 9 AND
               B.CALL_TYPE_CODE = 'G' THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ROAM_GPRS_ARPU_DAILY,
               SUM(CASE
               WHEN (B.SRC = 'CHR' AND B.CHARGE_TYPE IN ('O', 'R')) AND
               T.BUSINESS_SERVICE_KEY IS NULL AND DBST.REVENUE_STREAM = 9 THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS SUBS_ROAM_CONTENT_ARPU_DAILY,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_ONNET_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               1
               ELSE
               0
               END) AS V_OUT_ONNET_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_OUT_V_ONNET_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.ORIG_PARENT_OPERATOR_CODE = '130' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_ONNET_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.ORIG_PARENT_OPERATOR_CODE = '130' THEN
               1
               ELSE
               0
               END) AS V_IN_ONNET_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.ORIG_PARENT_OPERATOR_CODE = '130' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_IN_V_ONNET_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_OFFNET_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               1
               ELSE
               0
               END) AS V_OUT_OFFNET_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_OUT_V_OFFNET_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_OFFNET_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               1
               ELSE
               0
               END) AS V_IN_OFFNET_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_IN_V_OFFNET_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_FIX_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_OUT_FIX_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_OUT_V_FIX_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_FIX_LOC_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_IN_FIX_LOC_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_IN_V_FIX_LOC_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_INTER_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_OUT_INTER_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_OUT_V_INTER_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_INTER_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_IN_INTER_CALL_AMT,
               MAX(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.CALL_START_TIME
               ELSE
               NULL
               END) AS LAST_IN_V_INTER_CALL,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD2.COUNTRY_CODE = '7' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_INTER_RUS_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD2.COUNTRY_CODE = '998' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_INTER_UZB_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD2.COUNTRY_CODE = '996' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_INTER_KRGZ_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD1.COUNTRY_CODE = '7' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_INTER_RUS_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD1.COUNTRY_CODE = '998' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_INTER_UZB_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND DGD1.COUNTRY_CODE = '996' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_INTER_KRGZ_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_OUT_ROAM_CALL_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS V_IN_ROAM_CALL_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_ONNET_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_OFFNET_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_FIX_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '2' AND
               B.LOCATION_TYPE_KEY IN ('3') AND B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_OUT_INTER_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_ONNET_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_OFFNET_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY IN ('1', '2') AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_FIX_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'V' AND B.CALL_DIRECTION_IND = '1' AND
               B.LOCATION_TYPE_KEY IN ('3') AND B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ACTUAL_CALL_DURATION_SEC / 60
               ELSE
               0
               END) AS V_IN_INTER_ROAM_MIN,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'G' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND B.NETWORK_TYPE != 'L' AND
               C.CELL_NETTYPE NOT IN ('3G', '4G') THEN
               B.ROUNDED_DATA_VOLUME / 1024 / 1024
               ELSE
               0
               END) AS GPRS_DATA_MB_VLM_2G,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'G' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND B.NETWORK_TYPE != 'L' AND
               C.CELL_NETTYPE = ('3G') THEN
               B.ROUNDED_DATA_VOLUME / 1024 / 1024
               ELSE
               0
               END) AS GPRS_DATA_MB_VLM_3G,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'G' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               (B.NETWORK_TYPE = 'L' OR C.CELL_NETTYPE = ('4G')) THEN
               B.ROUNDED_DATA_VOLUME / 1024 / 1024
               ELSE
               0
               END) AS GPRS_DATA_MB_VLM_4G,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'G' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               B.ROUNDED_DATA_VOLUME / 1024 / 1024
               ELSE
               0
               END) AS GPRS_DATA_MB_VLM_ROAM,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               1
               ELSE
               0
               END) AS S_OUT_ONNET_LOC_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               1
               ELSE
               0
               END) AS S_OUT_OFFNET_LOC_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '2' AND
               B.CONNECTION_TYPE_KEY = '3' AND B.LOCATION_TYPE_KEY IN ('3') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS S_OUT_INTER_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '2' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS S_OUT_ROAM_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE = B.TERM_PARENT_OPERATOR_CODE AND
               B.TERM_PARENT_OPERATOR_CODE = '130' THEN
               1
               ELSE
               0
               END) AS S_IN_ONNET_LOC_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND
               B.LOCATION_TYPE_KEY IN ('1', '2') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' AND
               B.ORIG_PARENT_OPERATOR_CODE != B.TERM_PARENT_OPERATOR_CODE THEN
               1
               ELSE
               0
               END) AS S_IN_OFFNET_LOC_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '1' AND
               B.CONNECTION_TYPE_KEY = '3' AND B.LOCATION_TYPE_KEY IN ('3') AND
               B.SPECIAL_NUMBER_IND = '0' AND
               (B.ROAMING_IND = '0' AND B.ROAMING_TYPE_KEY IN ('H', 'X') OR
               B.ROAMING_IND = '1' AND B.ROAMING_TYPE_KEY = 'R') AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS S_IN_INTER_SMS_AMT,
               SUM(CASE
               WHEN B.SRC IN ('CHA', 'PSA', 'ROAM', 'GPRS', 'POST') AND
               B.CALL_TYPE_CODE = 'S' AND B.CALL_DIRECTION_IND = '1' AND
               (B.ROAMING_IND > '0' AND B.ROAMING_TYPE_KEY NOT IN ('R', 'X')) AND
               B.COUNTED_CDR_IND = '1' THEN
               1
               ELSE
               0
               END) AS S_IN_ROAM_SMS_AMT,
               SUM(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT > 0 THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS TOTAL_MFS_WRITE_OFF_AMT,
               MAX(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT > 0 THEN
               B.CHARGE_AMT + B.VAT_AMT
               ELSE
               0
               END) AS MAX_MFS_WRITE_OFF_AMT,
               SUM(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT > 0 THEN
               1
               ELSE
               0
               END) AS MAX_MFS_WRITE_OFF_COUNT,
               SUM(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT < 0 THEN
               ABS(B.CHARGE_AMT + B.VAT_AMT)
               ELSE
               0
               END) AS TOTAL_MFS_RECHARGE_AMT,
               MAX(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT < 0 THEN
               ABS(B.CHARGE_AMT + B.VAT_AMT)
               ELSE
               0
               END) AS MAX_MFS_RECHARGE_AMT,
               SUM(CASE
               WHEN B.SRC = 'CHR' AND
               DBS.BUSINESS_SERVICE_KEY IN (524506, 524507, 525058) AND
               B.CHARGE_TYPE = 'N' AND B.CHARGE_AMT < 0 THEN
               1
               ELSE 0
               END) AS MAX_MFS_RECHARGE_COUNT,
               SUM(CASE
               WHEN B.SRC = 'RHA' AND B.RECHARGE_SOURCE IN ('E', 'S', 'B') AND
               B.RECHARGE_AMT > 0 THEN
               B.RECHARGE_AMT
               ELSE 0 END) AS TOTAL_TOPUP_AMT,
               MAX(CASE
               WHEN B.SRC = 'RHA' AND B.RECHARGE_SOURCE IN ('E', 'S', 'B') AND
               B.RECHARGE_AMT > 0 THEN B.RECHARGE_AMT
               ELSE 0 END) AS MAX_TOPUP_AMT,
               SUM(CASE
               WHEN B.SRC = 'RHA' AND B.RECHARGE_SOURCE IN ('E', 'S', 'B') AND
               B.RECHARGE_AMT > 0 THEN 1
               ELSE 0 END) AS TOPUP_COUNT
               FROM det_layer B
               LEFT JOIN dim_business_service DBS
               ON B.BUSINESS_SERVICE_KEY = DBS.BUSINESS_SERVICE_KEY
               LEFT JOIN dim_business_service_type DBST
               ON DBS.BUSINESS_SERVICE_TYPE_KEY = DBST.BUSINESS_SERVICE_TYPE_KEY
               LEFT JOIN dim_bls_content_list T
               ON DBS.BUSINESS_SERVICE_KEY = T.BUSINESS_SERVICE_KEY
               AND DBST.BUSINESS_SERVICE_TYPE_KEY = T.BUSINESS_SERVICE_TYPE_KEY
               AND DBST.REVENUE_STREAM = T.REVENUE_STREAM
               LEFT JOIN dim_global_digit1 DGD1
               ON B.DIAL_DIGIT_KEY_FROM = DGD1.DIAL_DIGIT_KEY
               LEFT JOIN dim_global_digit2 DGD2
               ON B.DIAL_DIGIT_KEY_TO = DGD2.DIAL_DIGIT_KEY
               LEFT JOIN lcs_actual C
               ON TRIM(REGEXP_REPLACE(b.lac, '[^0-9]+', '')) = C.LAC
               AND TRIM(REGEXP_REPLACE(b.cell_id, '[^0-9]+', '')) = C.CELL_ID
               WHERE 1 = 1
               and B.CALL_START_TIME >= '${tools.addDays(P_DATE,-3)}'
               AND B.CALL_START_TIME < '${tools.addDays(P_DATE,1)}'
               GROUP BY B.SUBS_KEY,
               B.BAN_KEY,
               SUBSTR(B.CALL_START_TIME,1,10)
          """
          )

          df.write.parquet(subs_profile_ki_path + tools.patternToDate(tools.addDays(P_DATE,-3), "ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}