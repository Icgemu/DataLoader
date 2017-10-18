package cn.gaei.ev

import ch.hsr.geohash.WGS84Point
import ch.hsr.geohash.util.VincentyGeodesy
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, _}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by gz02559 on 2017/8/24.
  */
object OD {

  def main(args: Array[String]): Unit = {


    val input = args(0)
    val saveDir = args(1)
    val shufflePartitions = args(2).toInt
    val numPartitions = args(3).toInt
    val mem = args(4).toInt
    val core = args(5).toInt

    val sc = SparkSession
      .builder()
      //.appName("Spark SQL basic example")
      .config("spark.executor.memory", mem+"G")
      .config("spark.executor.cores", core)
      .config("spark.shuffle.spill", true)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.shuffle.spill.numElementsForceSpillThreshold", 1024 * 1024 * 1024 / 16)
      .config("spark.sql.windowExec.buffer.spill.threshold", 10*1024 * 1024)
      //.master("spark://master1:17077")
      .getOrCreate()


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
      } else {

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
    spark.udf.register("valid_od", (e: Seq[Int]) => {
      var res = 1
      if (e.size < 2) {
      }else {
        res = 0
      }
      res
    })

    //        val writer = new PrintWriter("LMGGN1S52E1000138_filter.csv")
    val winSpec = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 1)
    val winSpec1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec2 = Window.partitionBy($"vin", $"od_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val winSpec3 = Window.partitionBy($"vin", $"od_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec4 = Window.partitionBy($"vin",$"od_ts").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    var file = spark.read.parquet(input)

    val file1 = file
      .select($"vin", $"bcm_keyst", $"ts")
      .filter($"bcm_keyst".equalTo(2))
      //.withColumn("keyst", when($"bcm_keyst".equalTo(2), 1).otherwise(0))
//      .withColumn("year", $"date_str".substr(0, 4)).withColumn("month", $"date_str".substr(5, 2))
      //.filter($"keyst".notEqual(0))
      .withColumn("flags", callUDF("get_type1", collect_list($"ts").over(winSpec)))
      .withColumn("st_st_flags", callUDF("stst", collect_list($"flags").over(winSpec)))
      .withColumn("rev_st_st_flags", when($"st_st_flags".notEqual(2), 1).otherwise(0))
      .withColumn("batch_id", sum($"rev_st_st_flags").over(winSpec1))
      .withColumn("od_id", when($"st_st_flags".equalTo(1), $"batch_id" - 1).otherwise($"batch_id"))
      .withColumn("od_cnt", count("*").over(winSpec2))
      .filter($"od_cnt".gt(60))
      .filter($"st_st_flags".notEqual(2))
      .withColumn("od_ts", first($"ts").over(winSpec3))
      .withColumn("od_flag",callUDF("valid_od", collect_list($"st_st_flags").over(winSpec4)))
      .filter($"od_flag".equalTo(0))
      .select($"vin", $"ts", $"od_ts".as("od_id"), $"st_st_flags".as("start_stop_flag"))
      .repartition(numPartitions)
      .write.mode(SaveMode.Append)
      //.partitionBy("year", "month")
      .parquet(saveDir)

    //              .collect().foreach(e => writer.println(e.mkString(",")))
    //      .take(100).foreach(println)
    //          writer.close()
  }

  //  }
}
