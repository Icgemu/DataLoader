package cn.gaei.ev

import java.io.PrintWriter

import ch.hsr.geohash.WGS84Point
import ch.hsr.geohash.util.VincentyGeodesy
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, _}
import org.apache.spark.sql.types.LongType

/**
  * Created by gz02559 on 2017/8/24.
  */
object ODCalculate {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.executor.memory", "20G")
      .config("spark.executor.cores", "10")
      .config("spark.shuffle.spill", true)
      .config("spark.sql.shuffle.partitions", 400)
      .config("spark.sql.windowExec.buffer.spill.threshold", 1024 * 1024)
      //.master("spark://fushengrongdeMacBook-Pro.local:7077")
      .master("spark://master1:17077")
      .getOrCreate()

    //sc.sparkContext.setLogLevel("DEBUG")

    val spark = sc.newSession()
    import spark.implicits._

    spark.udf.register("get_type", (e: Seq[Seq[Long]]) => {
      var res = BigDecimal(0)
      if (e.size < 3) {
        res = BigDecimal(e(0)(0).toLong)
      } else {
        val a1 = e(0)(0)
        val a2 = e(1)(0)
        val a3 = e(2)(0)
        res = BigDecimal(a2.toLong)

        if (a1 == 1 && a2 == 1 && a3 == 1) {
          val t1 = e(0)(1)
          val t2 = e(1)(1)
          val t3 = e(2)(1)
          if ((t3 - t2) > 10 * 60 * 1000) {
            res = BigDecimal(0)
          }
        }
      }
      res.toInt
    })

    spark.udf.register("get_type1", (e: Seq[Long]) => {
      var res = 1
      if (e.size < 3) {
        //res = BigDecimal(e(0).toLong)

      } else {
        //        val a1 = e(0)
        //        val a2 = e(1)
        //        val a3 = e(2)
        //        res = a2.toInt

        //        if (a1 == 1 && a2 == 1 && a3 == 1) {
        //
        //        }
        val t1 = e(0)
        val t2 = e(1)
        val t3 = e(2)
        if ((t3 - t2) > 10 * 60 * 1000) {
          res = 0
        }
      }
      res
    })
    spark.udf.register("stst", (e: Seq[Int]) => {
      var res = 2
      if (e.size < 3) {
        //pass
      } else {
        val a1 = e(0)
        val a2 = e(1)
        val a3 = e(2)
        if (a1 == 0 && a2 == 1 && a3 == 1) {
          res = 0
        } else if (a1 == 1 && a2 == 1 && a3 == 0) {
          res = 1
        }
      }
      res
    })
    spark.udf.register("dist", (lon1: Double, lat1: Double, lon2: Double, lat2: Double) => {
      var res = 0.0
      val p1 = new WGS84Point(lat1, lon1)
      val p2 = new WGS84Point(lat2, lon2)
      res = VincentyGeodesy.distanceInMeters(p1, p2)
      res
    })

    //        val writer = new PrintWriter("LMGGN1S52E1000138_filter.csv")
    val winSpec = Window.partitionBy($"vin", $"year", $"month").orderBy($"ts").rowsBetween(-1, 1)
    val winSpec1 = Window.partitionBy($"vin", $"year", $"month").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec2 = Window.partitionBy($"vin", $"year", $"month", $"od_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val winSpec3 = Window.partitionBy($"vin", $"year", $"month", $"od_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    var file = spark.read.parquet("/data/parquet")
    //    for (i <- 0 until 10) {
    val file1 = file
      //         .filter($"vin".equalTo("LMGGN1S52E1000138"))
      //        .filter((hash($"vin") % 10).equalTo(i))
      .filter($"bcm_keyst".isNotNull && $"bcm_keyst".notEqual(0) && $"lon84".isNotNull && $"lon84".lt(180) && $"lat84".lt(90))
      //              .select($"vin", $"bcm_keyst", $"ts", from_unixtime($"ts" / 1000, "yyyy-MM-dd HH:mm:ss").as("ts_f"))
      .withColumn("keyst", when(($"bcm_keyst".isNull) || $"bcm_keyst".equalTo(0), 0).otherwise(1))
      .withColumn("year", $"date_str".substr(0, 4)).withColumn("month", $"date_str".substr(5, 2))
      .filter($"keyst".notEqual(0))
      //      .withColumn("keyst_ts", array($"keyst".cast(LongType),$"ts"))
      //      .withColumn("keyst_ts", array($"keyst",$"ts"))
      //      .withColumn("type", collect_list(array($"keyst".cast(LongType),$"ts")).over(winSpec))
      //        .select($"vin", $"bcm_keyst", $"ts",$"ts_f",callUDF("get_type",$"type").as("flags"))
      //        .withColumn("flags", callUDF("get_type", collect_list(array($"keyst".cast(LongType), $"ts")).over(winSpec)))
      .withColumn("flags", callUDF("get_type1", collect_list($"ts").over(winSpec)))

      //        .withColumn("flags_arr", array($"flags".cast(LongType),$"ts"))
      //        .withColumn("start_stop_list", collect_list($"flags").over(winSpec))
      //        .select($"vin" ,$"ts_f",$"ts",$"bcm_keyst",when($"flags".equalTo(0),"off").otherwise("_on"),callUDF("stst",$"start_stop_list").as("st_st_flags"))
      .withColumn("st_st_flags", callUDF("stst", collect_list($"flags").over(winSpec)))
      .withColumn("rev_st_st_flags", when($"st_st_flags".notEqual(2), 1).otherwise(0))
      .withColumn("batch_id", sum($"rev_st_st_flags").over(winSpec1))
      .withColumn("od_id", when($"st_st_flags".equalTo(1), $"batch_id" - 1).otherwise($"batch_id"))
      //        .filter($"keyst".notEqual(0))
      .withColumn("od_cnt", count("*").over(winSpec2))
      .filter($"od_cnt".gt(60))

      .withColumn("od_ts", first($"ts").over(winSpec3))
      .drop("keyst", "flags", "rev_st_st_flags", "batch_id", "od_cnt", "od_id")
      .repartition(10)
      //       .select($"vin",$"keyst", $"flags", $"rev_st_st_flags", $"batch_id", $"od_cnt", $"od_id")
      //        .filter($"st_st_flags".notEqual(2))
      //          .select($"vin", $"ts",$"ts_f",$"lon84",$"lat84",$"st_st_flags"
      //            , lead($"ts",1).over(winSpec1).as("ts_1")
      //            , lead($"ts_f",1).over(winSpec1).as("ts_f1")
      //            , lead($"lon84",1).over(winSpec1).as("lon84_1")
      //            , lead($"lat84",1).over(winSpec1).as("lat84_1")
      //            , lead($"st_st_flags",1).over(winSpec1).as("st_st_flags_1")
      //          )
      //        .filter($"st_st_flags".equalTo(0) && $"st_st_flags_1".equalTo(1))
      //        .withColumn("dist",callUDF("dist",$"lon84",$"lat84",$"lon84_1",$"lat84_1"))
      //        .filter($"dist".gt(100)&& ($"ts" - $"ts_1").gt(10*60*1000))
      //        .explain(true)
      .write.mode(SaveMode.Append).partitionBy("year", "month").parquet("/data/od/")

    //              .collect().foreach(e => writer.println(e.mkString(",")))
    //      .take(100).foreach(println)
    //          writer.close()
  }

  //  }
}
