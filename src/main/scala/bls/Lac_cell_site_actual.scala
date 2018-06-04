package bls

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Lac_cell_site_actual(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  var status = true
  val lcs_path = "/dmp/daily_stg/bls/lac_cell_site_actual"

  def RunJob():Boolean= {
    if (spark != null)
      try {
        tools.removeFolder(lcs_path)
        val ds = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_sites")
        val dc = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_city")
        val dct = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_city_type")
        val da = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_area")
        val dr = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_region")
        val db = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_bts")
        val dbts = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_btsstatus")
        val dbt = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_btstype")
        val r = spark.read.parquet("/mnt/gluster-storage/etl/download/ncsa/dir_sector")

        val ndb = ds.join(dc, ds("addr_id_city") === dc("id_city")).join(dct, dc("id_city_type") === dct("id_city_type"))
          .join(da, dc("id_area") === da("id_area")).join(dr, da("id_region") === dr("id_region")).join(db, ds("site_id") === db("site_id"))
          .join(dbts, db("id_btsstatus") === dbts("id_btsstatus")).join(dbt, db("id_btstype") === dbt("id_btstype"))
          .join(r, db("id_bts") === r("id_bts") && r("cell_nettype").isin("2G", "3G") && r("cell_status").isin("test", "active")).select(
          dr("name_region").as("region_name"),
          da("area_name").as("district_name"),
          dc("city_name").as("city_name"),
          dct("city_type_name").as("city_type_name"),
          ds("site_addr"),
          ds("longitude"),
          ds("latitude"),
          db("inservice_date"),
          r("lac_tac").as("lac_tac"),
          r("cell_id").as("cell_id"),
          r("cell_nettype").as("cell_nettype")
        ).union(
          ds.join(dc, ds("addr_id_city") === dc("id_city")).join(dct, dc("id_city_type") === dct("id_city_type"))
            .join(da, dc("id_area") === da("id_area")).join(dr, da("id_region") === dr("id_region")).join(db, ds("site_id") === db("site_id"))
            .join(dbts, db("id_btsstatus") === dbts("id_btsstatus")).join(dbt, db("id_btstype") === dbt("id_btstype"))
            .join(r, db("id_bts") === r("id_bts") && r("cell_nettype").isin("4G") && r("cell_status").isin("test", "active")).select(
            dr("name_region").as("region_name"),
            da("area_name").as("district_name"),
            dc("city_name").as("city_name"),
            dct("city_type_name").as("city_type_name"),
            ds("site_addr"),
            ds("longitude"),
            ds("latitude"),
            db("inservice_date"),
            r("lac_tac").as("lac_tac"),
            db("enodeb_id").as("cell_id"),
            r("cell_nettype")
          )
        )

        val dlc = ndb.select(
          $"region_name",
          $"district_name",
          $"city_name",
          $"city_type_name",
          $"site_addr",
          $"longitude",
          $"latitude",
          $"inservice_date",
          $"lac_tac",
          $"cell_id",
          $"cell_nettype",
          row_number().over(Window.partitionBy($"lac_tac", $"cell_id").orderBy($"inservice_date".desc)).as("RN")
        )

        val dlca = dlc.filter($"rn" === 1).select(
          $"region_name".as("region"),
          $"district_name".as("district"),
          $"city_name".as("city"),
          $"city_type_name".as("city_type"),
          $"lac_tac".as("lac"),
          $"cell_id",
          $"cell_nettype"
        ).write.parquet(lcs_path)

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}