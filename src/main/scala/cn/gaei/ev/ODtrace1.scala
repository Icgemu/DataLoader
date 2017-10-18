package cn.gaei.ev

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, _}
import org.apache.spark.sql.types.LongType

/**
  * Created by gz02559 on 2017/8/24.
  */
object ODtrace1 {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.executor.memory", "16G")
      .config("spark.executor.cores", "8")
      //.master("spark://fushengrongdeMacBook-Pro.local:7077")
      .master("spark://master1:17077")
      .getOrCreate()


    val spark = sc.newSession()
    import spark.implicits._

    spark.udf.register("get_type", (e: Seq[Seq[Long]]) => {
      var res = BigDecimal(0)
      if(e.size < 3){
        res = BigDecimal(e(0)(0).toLong)
      }else{
        val a1 = e(0)(0)
        val a2 = e(1)(0)
        val a3 = e(2)(0)
        res = BigDecimal(a2.toLong)

        if(a1 == 1 && a2 ==1 && a3==1 ){
          val t1 = e(0)(1)
          val t2 = e(1)(1)
          val t3 = e(2)(1)
          if((t3 - t2) > 10*60*1000 ){
            res = BigDecimal(0)
          }
        }
      }
      res.toInt
    })

    spark.udf.register("get_type1", (a1: Int,a2:Int,a3:Int,t1:Long,t2:Long,t3:Long) => {
      var res:Long = 0

      if(a1 == null  || a3 == null){
        res = a2
      }else{
        res = a2
        if(a1 == 1 && a2 ==1 && a3==1 ){
          if((t3 - t2) > 10*60*1000 ){
            res = 0
          }
        }
      }
//      if(e.size < 3){
//        res = BigDecimal(e(0)(0).toLong)
//      }else{
//        val a1 = e(0)(0)
//        val a2 = e(1)(0)
//        val a3 = e(2)(0)
//        res = BigDecimal(a2.toLong)
//
//        if(a1 == 1 && a2 ==1 && a3==1 ){
//          val t1 = e(0)(1)
//          val t2 = e(1)(1)
//          val t3 = e(2)(1)
//          if((t3 - t2) > 10*60*1000 ){
//            res = BigDecimal(0)
//          }
//        }
//      }
      res.toInt
    })

    spark.udf.register("stst", (e: Seq[Int]) => {
      var res = 2
      if(e.size < 3){
        //pass
      }else {
        val a1 = e(0)
        val a2 = e(1)
        val a3 = e(2)
        if (a1 == 0 && a2 == 1 && a3 == 1) {
          res = 0
        }else if(a1 == 1 && a2 == 1 && a3 == 0){
          res = 1
        }
      }
      res
    })
    spark.udf.register("stst1", (a1: Int,a2:Int,a3:Int) => {
      var res = 2

      if(a1 == null  || a3 == null){
        //pass
      }else{
        if (a1 == 0 && a2 == 1 && a3 == 1) {
          res = 0
        }else if(a1 == 1 && a2 == 1 && a3 == 0){
          res = 1
        }
      }
//      if(e.size < 3){
//        //pass
//      }else {
//        val a1 = e(0)
//        val a2 = e(1)
//        val a3 = e(2)
//        if (a1 == 0 && a2 == 1 && a3 == 1) {
//          res = 0
//        }else if(a1 == 1 && a2 == 1 && a3 == 0){
//          res = 1
//        }
//      }
      res
    })
    val writer = new PrintWriter("all_filter.csv")
    val winSpec = Window.partitionBy($"vin").orderBy($"ts")
//    val winSpec1 = Window.partitionBy($"vin").orderBy($"ts")
    var file = spark.read.parquet("/data/parquet")
    val file1 = file
      //    .filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".equalTo(0) && $"vin".equalTo("LMGGN1S52E1000138"))
//      .filter($"vin".equalTo("LMGGN1S52E1000138"))
      .select($"vin", $"bcm_keyst", $"ts", from_unixtime($"ts" / 1000, "yyyy-MM-dd HH:mm:ss").as("ts_f"))
      .withColumn("keyst",when($"bcm_keyst".isNull,0).otherwise(1))
      .withColumn("keyst1",lag($"keyst",1).over(winSpec)).withColumn("keyst2",lead($"keyst",1).over(winSpec))
      .withColumn("ts1",lag($"ts",1).over(winSpec)).withColumn("ts2",lead($"ts",1).over(winSpec))
      .select($"vin", $"bcm_keyst", $"ts",$"ts_f",callUDF("get_type1",$"keyst1",$"keyst",$"keyst2",$"ts1",$"ts",$"ts2").as("flags"))
      //.withColumn("keyst_ts", array($"keyst".cast(LongType),$"ts"))
//      .withColumn("keyst_ts", array($"keyst",$"ts"))
      //.withColumn("type", collect_list($"keyst_ts").over(winSpec))
//        .select($"vin", $"bcm_keyst", $"ts",$"ts1",callUDF("get_type",$"type").as("flags"),$"lon84",$"lat84")
//        .withColumn("flags_arr", array($"flags".cast(LongType),$"ts"))
      .withColumn("flags1",lag($"flags",1).over(winSpec)).withColumn("flags2",lead($"flags",1).over(winSpec))
//        .withColumn("start_stop_list", collect_list($"flags").over(winSpec))
        .select($"vin" ,$"ts_f",callUDF("stst1",$"flags1",$"flags",$"flags2").as("st_st_flags"))
        .filter($"st_st_flags".notEqual(2))
//        .explain()
      .write.csv("/data/od/")

        //.collect().foreach(e => writer.println(e.mkString(",")))
//      .take(100).foreach(println)
    writer.close()
  }
}
