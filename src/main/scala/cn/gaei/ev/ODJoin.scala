package cn.gaei.ev

import java.io.PrintWriter

import ch.hsr.geohash.WGS84Point
import ch.hsr.geohash.util.VincentyGeodesy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object ODJoin {

  def main(args: Array[String]): Unit = {
    val inputorig = args(0)
    val inputOD = args(1)
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
//      .config("spark.memory.fraction", 0.8)
//      .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 720)
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.spill.numElementsForceSpillThreshold", 1024 * 1024 * 1024 / 8)
      .config("spark.sql.windowExec.buffer.spill.threshold", 20 * 1024 * 1024)
      //.master("spark://master1:17077")
      .getOrCreate()


    val spark = sc.newSession()
    import spark.implicits._

    spark.udf.register("dist", (e:Seq[Seq[Double]]) => {
      var res = 0.0
      val last = e(0)
      var p1 = new WGS84Point(last(1), last(0))
      val end = e.tail
      end.map(m =>{
        val p2 = new WGS84Point(m(1), m(0))
        res += VincentyGeodesy.distanceInMeters(p1, p2)
        p1 = p2
      })
      res
    })

    val winSpec1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec2 = Window.partitionBy($"vin",$"seq_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec3 = Window.partitionBy($"vin",$"seq_id").orderBy($"ts").rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val orig = spark.read.parquet(inputorig).filter($"bcm_keyst".isNotNull && $"bcm_keyst".notEqual(0)).select($"vin", $"ts",$"lon84",$"lat84")
      //.filter($"vin".equalTo("LMGGN1S52E1000138"))
    val od = spark.read.parquet(inputOD)
      //.filter($"vin".equalTo("LMGGN1S52E1000138"))
//    val writer = new PrintWriter("LMGGN1S52E1000138_Join.csv")
    orig
      .join(od, Seq("vin","ts"),"left_outer")
      .withColumn("flag",when($"start_stop_flag".isNull, 0).otherwise(1))
      .withColumn("batch_id", sum($"flag").over(winSpec1))
      .withColumn("seq_id", when($"start_stop_flag".equalTo(1), $"batch_id" - 1).otherwise($"batch_id"))
      .withColumn("od_ts", first($"od_id").over(winSpec2))
      .withColumn("od_end", last($"ts").over(winSpec3))
      .filter($"od_ts".isNotNull && $"lon84".isNotNull && $"lon84".lt(180) && $"lat84".lt(90)&& $"lon84".gt(100) && $"lat84".gt(20))
      //.withColumn("tstr",from_unixtime($"ts" / 1000, "yyyy-MM-dd HH:mm:ss"))
      .select($"vin",$"od_ts",$"od_end",$"lon84",$"lat84")
      .groupBy($"vin",$"od_ts",$"od_end").agg(callUDF("dist", collect_list(array($"lon84",$"lat84"))).as("dist"))
      .withColumn("start_dstr",from_unixtime($"od_ts" / 1000, "yyyy-MM-dd HH:mm:ss") )
      .withColumn("end_dstr",from_unixtime($"od_end" / 1000, "yyyy-MM-dd HH:mm:ss") )
      .select($"vin",$"start_dstr",$"end_dstr",$"dist")
      //.orderBy($"start_dstr")
      .write.parquet("/data/dist")
      //.explain()

//      .collect().map(d => writer.write(d.mkString(",")+"\n"))
//    writer.close()
  }

}
