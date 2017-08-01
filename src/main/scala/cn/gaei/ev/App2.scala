package cn.gaei.ev

import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * @author ${user.name}
  */
object App2 {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    //      .setMaster("spark://master1:17077")
    //      .setAppName("CountingSheep")
    //      .set("spark.executor.memory","4G")
    //      .set("spark.executor.cores","4")
    //      .set("spark.cores.max","20")
    //    val sc = new SparkContext(conf)

    //    val file = sc.textFile("hdfs://gaei/data/uniq/*/*")

    //    println(file.count
    val buff = new StringBuilder

    buff ++= "VIN  STRING ,TS LONG,DATE_STR STRING,BMS_BATTST INT,BMS_BATTCURR  FLOAT,"
    buff ++= "BMS_BATTVOLT  INT,BMS_INSULATIONST  INT,BMS_INSULATIONRES  INT,"
    buff ++= "BMS_CELLVOLTMAX  FLOAT,BMS_CELLVOLTMIN  FLOAT,BMS_FAILURELVL  INT,"
    buff ++= "BMS_BATTSOC  FLOAT,BMS_BATTTEMPAVG  FLOAT,BMS_BATTTEMPMAX  FLOAT,"
    buff ++= "BMS_BATTTEMPMIN  FLOAT,CCS_CHARGEVOLT  FLOAT,CCS_CHARGECUR  FLOAT,CCS_CHARGERSTARTST  INT,"
    buff ++= "VCU_SYSFAILMODE  INT,MCU_FTM_ACTROTSPD  INT,MCU_FTM_ACTTORQ  FLOAT,MCU_FTM_STMODE  INT,"
    buff ++= "MCU_FTM_MOTORACTTEMP  INT,MCU_FTM_ROTORACTTEMP  INT,MCU_FTM_INVERTERACTTEMP  INT,"
    buff ++= "MCU_FTM_ACTHV_CUR  INT,MCU_FTM_ACTHV_VOLT  INT,MCU_FTM_FAULT_INFO1  INT,MCU_FTM_FAULT_INFO2  INT,"
    buff ++= "MCU_DCDC_FAILST  INT,MCU_DCDC_STMODE  INT,VCU_DCDC_STMODELREQ  INT,BMS_BAT_ERROR_SOC_L  INT,"
    buff ++= "BMS_BAT_ERROR_CELL_V_H  INT,BMS_BAT_ERROR_CELL_V_L  INT,BMS_BAT_ERROR_PACK_SUMV_H  INT,"
    buff ++= "BMS_BAT_ERROR_PACK_SUMV_L  INT,BMS_BAT_ERROR_CELL_T_H  INT,BMS_BAT_ERROR_T_UNBALANCE  INT,"
    buff ++= "MCU_GM_FAILST  INT,MCU_FTM_FAILST  INT,MCU_FTM_FAULT_INFO3  INT,MCU_GM_ACTROTSPD  INT,"
    buff ++= "MCU_GM_ACTTORQ  FLOAT,MCU_GM_STMODE  INT,MCU_GM_MOTORACTTEMP  INT,MCU_GM_ROTORACTTEMP  INT,"
    buff ++= "MCU_GM_INVERTERACTTEMP  INT,MCU_GM_ACTHV_CUR  INT,MCU_GM_ACTHV_VOL  INT,EMS_ENGTORQ  FLOAT,"
    buff ++= "EMS_ENGSPD  INT,EMS_ACCPEDALPST  FLOAT,EMS_BRAKEPEDALST  INT,EMS_ENGWATERTEMP  INT,HCU_GEARFORDSP  INT,"
    buff ++= "HCU_OILPRESSUREWARN  INT,HCU_AVGFUELCONSUMP  FLOAT,HCU_BATCHRGDSP  INT,BCS_VEHSPD  FLOAT,"
    buff ++= "ICM_TOTALODOMETER  INT,BCM_KEYST  INT,HCU_DSTOIL  INT,HCU_DSTBAT  INT,EMS_FAULTRANKSIG  INT,"
    buff ++= "SRS_CRASHOUTPUTST  INT,SRS_DRIVERSEATBELTST  INT,EDC_STERRLVLCOM  INT,EDC_STERRLVLCOMSUP  INT,"
    buff ++= "EDB_STERRLVLHVES  INT,EDG_STERRLVLGEN  INT,EDM_STERRLVLMOT  INT,EDE_STERRLVLENG  INT,"
    buff ++= "EDV_STERRLVLVEH  INT,LON84  FLOAT,LAT84  FLOAT,LON02  FLOAT,LAT02  FLOAT,BCS_ABSFAULTST  INT,"
    buff ++= "BCS_EBDFAULTST  INT,MCU_DCDC_ACTTEMP  FLOAT,BMS_HVILST  INT,HCU_HEVSYSREADYST  INT,BMS_BALANCEST  INT,"
    buff ++= "GPS_HEADING  FLOAT"

    val schema_str = buff.toString().toLowerCase.split(",")
    var type_map = Map("string" -> StringType, "int"-> IntegerType, "long" ->LongType,"float" -> DoubleType )
    val schema: Array[StructField] = schema_str.map(e => {
      val field_str: Array[String] = e.split("\\s+")
      println(field_str(0) + "->" + field_str(1))
      val field_name = field_str(0).toLowerCase.trim
      val field_type = field_str(1).toLowerCase.trim
      StructField(field_name, type_map(field_type), true)
    })
    //    val buff = new ArrayBuffer()
    //    val schema= StructType(Array(
    //             StructField("cp_catalog_page_sk",IntegerType,false),
    //             StructField("cp_start_date_sk",IntegerType,true),
    //             StructField("cp_end_date_sk",IntegerType,true),
    //             StructField("cp_department",StringType,true),
    //             StructField("cp_catalog_number",LongType,true),
    //             StructField("cp_catalog_page_number",LongType,true),
    //             StructField("cp_description",StringType,true),
    //             StructField("cp_type",StringType,true)))

    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.executor.memory", "4G")
      .config("spark.executor.cores", "4")
      .master("spark://master1:17077")
      .getOrCreate()

    val lines = sc.sparkContext.newAPIHadoopFile("hdfs://gaei/data/uniq/2016/ag_vin_2016_01_uniq.csv.lzo",
      classOf[LzoTextInputFormat],classOf[LongWritable],classOf[Text]).map(_._2.toString)
    val mpp = schema_str.map(_.split("\\s+"))
//    val schema = mpp.map(e=>e(0)).mkString(" ")
    val rdd = lines.map(line =>{
       val str:Array[String] = line.split(",")
       var row = new ArrayBuffer[Any]()
      for((x,i) <- str.view.zipWithIndex) {
        val field_type = mpp[i](1)
        val field = field_type match {
          case "string" => x.trim();
          case "long" => x.trim.toLong;
          case "float" => x.toDouble;
          case "int" => x.toInt;
        }
        row += (field)
      }
      Row.fromSeq(row.toSeq)
    })
    val spark = sc.newSession()
    import spark.implicits._
    spark.createDataFrame(rdd,StructType(schema))
    file.write.mode(SaveMode.Append).parquet("hdfs://gaei/data/parquet/")
    // For implicit conversions like converting RDDs to DataFrames


  }

}
