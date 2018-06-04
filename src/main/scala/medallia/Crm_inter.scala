package medallia

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Crm_inter(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val crm_path = "/dmp/daily_stg/medallia/crm/"

  def RunJob():Boolean= {
    if (spark != null)
      try {

        val ti = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_interact/*/").createOrReplaceTempView("ti")
        val ts = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_site/*/").createOrReplaceTempView("ts")
        val tsp = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_site_part/*/").createOrReplaceTempView("tsp")
        val tbo = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_bus_org/*/").createOrReplaceTempView("tbo")
        val txrat = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_x_ref_account_type/*/").createOrReplaceTempView("txrat")
        val txt = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_interact_txt/*/").createOrReplaceTempView("txt")
        val txir = spark.read.parquet("/mnt/gluster-storage/etl/download/crm/table_x_int_reason/*/").createOrReplaceTempView("txir")

        val df1 = spark.sql("""
        select
        ti.objid,
        tatp.serial_no ctn,
        tatp.account_type,
        case
        when tatp.account_type in ('13', '113', '37', '39') then
        'B2C'
        when tatp.account_type in ('41') then
        'B2B'
        else
        null
        end as segment_type,
        ti.end_date InteractionEndTime,
        ti.inserted_by,
        (select x_title from table_x_int_reason where objid = ti.x_interact2reason1) InteractionReasons1,
        (select x_title from table_x_int_reason where objid = ti.x_interact2reason2) InteractionReasons2,
        (select x_title from table_x_int_reason where objid = ti.x_interact2reason3) InteractionReasons3,
        (select x_title from table_x_int_reason where objid = ti.x_interact2reason4) InteractionReasons4,
        ti.interact_id InteractionID,
        regexp_extract(txt.notes, '([^*]+)', 1) PosDealerCode,
        sysdate CreationDate
        from table_interact ti,
        (select tsp.objid, tsp.serial_no, txrat.account_type
        from table_site               ts,
        table_site_part          tsp,
        table_bus_org            tbo,
        table_x_ref_account_type txrat
        where tsp.site_part2site = ts.objid
        and ts.primary2bus_org = tbo.objid
        and tbo.type = txrat.account_type_desc
        and tsp.part_status = 'Active') tatp,
        table_interact_txt txt
        where tatp.objid = ti.interact2site_part
        and txt.interact_txt2interact = ti.objid
        and ti.inserted_by <> 'robot'
        and ti.direction = 'Исходящий'
        and ti.type = 'Email'
        and ti.create_date > date_sub(current_date,-10)
        """).cache()

        val df2 = spark.sql(
          """
           select
            ti.objid,
            tatp.serial_no ctn,
            tatp.account_type,
            case
              when tatp.account_type in ('13', '113', '37', '39') then
               'B2C'
              when tatp.account_type in ('41') then
               'B2B'
              else
               null
            end as segment_type,
            ti.end_date InteractionEndTime,
            ti.inserted_by,
            (select x_title from table_x_int_reason where objid = ti.x_interact2reason1) InteractionReasons1,
            (select x_title from table_x_int_reason where objid = ti.x_interact2reason2) InteractionReasons2,
            (select x_title from table_x_int_reason where objid = ti.x_interact2reason3) InteractionReasons3,
            (select x_title from table_x_int_reason where objid = ti.x_interact2reason4) InteractionReasons4,
            ti.interact_id InteractionID,
            regexp_extract(ti.title, '([^;]+)', 1) PosDealerCode,
            sysdate CreationDate
             from table_interact ti,
                  (select tsp.objid, tsp.serial_no, txrat.account_type
                     from table_site               ts,
                          table_site_part          tsp,
                          table_bus_org            tbo,
                          table_x_ref_account_type txrat
                    where tsp.site_part2site = ts.objid
                      and ts.primary2bus_org = tbo.objid
                      and tbo.type = txrat.account_type_desc
                      and tsp.part_status = 'Active') tatp
            where tatp.objid = ti.interact2site_part
              and ti.inserted_by = 'robot'
              and ti.direction = 'Входящий'
              and ti.type = 'Email'
              and ti.create_date > date_sub(current_date,-10)
          """
        ).cache()

        df1.union(df2).filter($"account_type".isin(13,113,37,39,41))
            .write.parquet(crm_path + tools.patternToDate(P_DATE,"ddMMyyyy"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}