package cn.gaei.ev

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.elasticsearch.spark.sql._

object EsReader {

  def main(args: Array[String]): Unit = {
    val format=new SimpleDateFormat("yyyy-MM-dd")
    val d_str = args(0)
    val t1 = format.parse(d_str).getTime()
    val t2 = t1 + 24*60*60*1000

    val query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"TDATE\":{\"gt\":\"" + t1 + "\",\"lt\":\"" + t2 + "\"}}}]}}}"
    val sc = SparkSession
      .builder()
      .config("spark.executor.memory", "4G")
      .config("spark.executor.cores", 2)
      .config("es.nodes", "slave1:19200, slave2:19200, master1:19200")
      .config("es.query", query)
      .getOrCreate()

    val spark = sc.newSession()
    val sql = spark.sqlContext
    import spark.implicits._

    val ind = d_str.substring(0,7).replace("-",".")
    println("index=>"+ind)
    val signals = EsSparkSQL.esDF(sql, "ev."+ind+"/signals")

    signals
      .withColumn("LAT02",split($"LOCATION02",",")(0))
      .withColumn("LON02",split($"LOCATION02",",")(1))
      .withColumn("LAT84",split($"LOCATION84",",")(0))
      .withColumn("LON84",split($"LOCATION84",",")(1))

      .select(
        $"VIN",$"TDATE",$"SDATE",$"BMS_BATTST",$"BMS_BATTCURR",$"BMS_BATTVOLT",$"BMS_INSULATIONST",$"BMS_INSULATIONRES",$"BMS_CELLVOLTMAX",$"BMS_CELLVOLTMIN",
        $"BMS_FAILURELVL",$"BMS_BATTSOC",$"BMS_BATTTEMPAVG",$"BMS_BATTTEMPMAX",$"BMS_BATTTEMPMIN",$"CCS_CHARGEVOLT",$"CCS_CHARGECUR",$"CCS_CHARGERSTARTST",$"VCU_SYSFAILMODE",$"MCU_FTM_ACTROTSPD",
        $"MCU_FTM_ACTTORQ",$"MCU_FTM_STMODE",$"MCU_FTM_MOTORACTTEMP",$"MCU_FTM_ROTORACTTEMP",$"MCU_FTM_INVERTERACTTEMP",$"MCU_FTM_ACTHV_CUR",$"MCU_FTM_ACTHV_VOLT",$"MCU_FTM_FAULT_INFO1",$"MCU_FTM_FAULT_INFO2",$"MCU_DCDC_FAILST",
        $"MCU_DCDC_STMODE",$"VCU_DCDC_STMODELREQ",$"BMS_BAT_ERROR_SOC_L",$"BMS_BAT_ERROR_CELL_V_H",$"BMS_BAT_ERROR_CELL_V_L",$"BMS_BAT_ERROR_PACK_SUMV_H",$"BMS_BAT_ERROR_PACK_SUMV_L",$"BMS_BAT_ERROR_CELL_T_H",$"BMS_BAT_ERROR_T_UNBALANCE",$"MCU_GM_FAILST",
        $"MCU_FTM_FAILST",$"MCU_FTM_FAULT_INFO3",$"MCU_GM_ACTROTSPD",$"MCU_GM_ACTTORQ",$"MCU_GM_STMODE",$"MCU_GM_MOTORACTTEMP",$"MCU_GM_ROTORACTTEMP",$"MCU_GM_INVERTERACTTEMP",$"MCU_GM_ACTHV_CUR",$"MCU_GM_ACTHV_VOL",
        $"EMS_ENGTORQ",$"EMS_ENGSPD",$"EMS_ACCPEDALPST",$"EMS_BRAKEPEDALST",$"EMS_ENGWATERTEMP",$"HCU_GEARFORDSP",$"HCU_OILPRESSUREWARN",$"HCU_AVGFUELCONSUMP",$"HCU_BATCHRGDSP",$"BCS_VEHSPD",
        $"ICM_TOTALODOMETER",$"BCM_KEYST",$"HCU_DSTOIL",$"HCU_DSTBAT",$"EMS_FAULTRANKSIG",$"SRS_CRASHOUTPUTST",$"SRS_DRIVERSEATBELTST",$"EDC_STERRLVLCOM",$"EDC_STERRLVLCOMSUP",$"EDB_STERRLVLHVES",
        $"EDG_STERRLVLGEN",$"EDM_STERRLVLMOT",$"EDE_STERRLVLENG",$"EDV_STERRLVLVEH",$"LON84",$"LAT84",$"LON02",$"LAT02",$"BCS_ABSFAULTST",$"BCS_EBDFAULTST",
        $"MCU_DCDC_ACTTEMP",$"BMS_HVILST",$"HCU_HEVSYSREADYST",$"BMS_BALANCEST",$"GPS_HEADING",lit(","))
        .na.fill("")
    //signals.filter($"VIN".equalTo("LMGGN1S52E1000138") && to_date($"SDATE", "yyyy-MM-dd").equalTo(d_str)).select($"VIN",$"SDATE",$"TDATE").take(10).foreach(e => println(e.mkString(",")))
    //signals.select($"VIN",$"SDATE",$"TDATE")
      .take(10).foreach(e => println(e.mkString(",")))
//    signals.rdd.map( transf )
  }

//  String output = map.get("VIN")+","+map.get("TDATE")+","+map.get("SDATE").replace("-","")+","+map.get("BMS_BATTST")+","+
//    map.get("BMS_BATTCURR")+","+map.get("BMS_BATTVOLT")+","+map.get("BMS_INSULATIONST")+","+map.get("BMS_INSULATIONRES")+","+
//    map.get("BMS_CELLVOLTMAX")+","+map.get("BMS_CELLVOLTMIN")

  // +","+map.get("BMS_FAILURELVL")+","+map.get("BMS_BATTSOC")+","+
//    map.get("BMS_BATTTEMPAVG")+","+map.get("BMS_BATTTEMPMAX")+","+map.get("BMS_BATTTEMPMIN")+","+map.get("CCS_CHARGEVOLT")+","+
//    map.get("CCS_CHARGECUR")+","+map.get("CCS_CHARGERSTARTST")+","+map.get("VCU_SYSFAILMODE")+","+map.get("MCU_FTM_ACTROTSPD")+","+

//    map.get("MCU_FTM_ACTTORQ")+","+map.get("MCU_FTM_STMODE")+","+map.get("MCU_FTM_MOTORACTTEMP")+","+map.get("MCU_FTM_ROTORACTTEMP")+","+
//    map.get("MCU_FTM_INVERTERACTTEMP")+","+map.get("MCU_FTM_ACTHV_CUR")+","+map.get("MCU_FTM_ACTHV_VOLT")+","+map.get("MCU_FTM_FAULT_INFO1")+","+
//    map.get("MCU_FTM_FAULT_INFO2")+","+map.get("MCU_DCDC_FAILST")+","
  //
  // +map.get("MCU_DCDC_STMODE")+","+map.get("VCU_DCDC_STMODELREQ")+","+
//    map.get("BMS_BAT_ERROR_SOC_L")+","+map.get("BMS_BAT_ERROR_CELL_V_H")+","+map.get("BMS_BAT_ERROR_CELL_V_L")+","+map.get("BMS_BAT_ERROR_PACK_SUMV_H")+","+
//    map.get("BMS_BAT_ERROR_PACK_SUMV_L")+","+map.get("BMS_BAT_ERROR_CELL_T_H")+","+map.get("BMS_BAT_ERROR_T_UNBALANCE")+","+map.get("MCU_GM_FAILST")+","+

//    map.get("MCU_FTM_FAILST")+","+map.get("MCU_FTM_FAULT_INFO3")+","+map.get("MCU_GM_ACTROTSPD")+","+map.get("MCU_GM_ACTTORQ")+","+
//    map.get("MCU_GM_STMODE")+","+map.get("MCU_GM_MOTORACTTEMP")+","+map.get("MCU_GM_ROTORACTTEMP")+","+map.get("MCU_GM_INVERTERACTTEMP")+","+
//    map.get("MCU_GM_ACTHV_CUR")+","+map.get("MCU_GM_ACTHV_VOL")+","
  //
  // +map.get("EMS_ENGTORQ")+","+map.get("EMS_ENGSPD")+","+
//    map.get("EMS_ACCPEDALPST")+","+map.get("EMS_BRAKEPEDALST")+","+map.get("EMS_ENGWATERTEMP")+","+map.get("HCU_GEARFORDSP")+","+
//    map.get("HCU_OILPRESSUREWARN")+","+map.get("HCU_AVGFUELCONSUMP")+","+map.get("HCU_BATCHRGDSP")+","+map.get("BCS_VEHSPD")+","+

//    map.get("ICM_TOTALODOMETER")+","+map.get("BCM_KEYST")+","+map.get("HCU_DSTOIL")+","+map.get("HCU_DSTBAT")+","+
//    map.get("EMS_FAULTRANKSIG")+","+map.get("SRS_CRASHOUTPUTST")+","+map.get("SRS_DRIVERSEATBELTST")+","+map.get("EDC_STERRLVLCOM")+","+
//    map.get("EDC_STERRLVLCOMSUP")+","+map.get("EDB_STERRLVLHVES")+","
  //
  // +map.get("EDG_STERRLVLGEN")+","+map.get("EDM_STERRLVLMOT")+","+
//    map.get("EDE_STERRLVLENG")+","+map.get("EDV_STERRLVLVEH")+","+map.get("LON84")+","+map.get("LAT84")+","+
//    map.get("LON02")+","+map.get("LAT02")+","+map.get("BCS_ABSFAULTST")+","+map.get("BCS_EBDFAULTST")+","+

//    map.get("MCU_DCDC_ACTTEMP")+","+map.get("BMS_HVILST")+","+map.get("HCU_HEVSYSREADYST")+","+map.get("BMS_BALANCEST")+","+
//    map.get("GPS_HEADING")+",";
}
