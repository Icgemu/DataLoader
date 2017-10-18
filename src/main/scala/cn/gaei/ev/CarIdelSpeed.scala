package cn.gaei.ev

import ch.hsr.geohash.{GeoHash, WGS84Point}
import ch.hsr.geohash.util.VincentyGeodesy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.Try

object CarIdelSpeed {

  def main(args: Array[String]): Unit = {
    val inputorig = args(0)
    val inputcar = args(1)
    val year = args(2).toInt
    val month = args(3).toInt
    val mem = args(4).toInt
    val core = args(5).toInt

    val sc = SparkSession
      .builder()
      //.appName("Spark SQL basic example")
      .config("spark.executor.memory", mem+"G")
      .config("spark.executor.cores", core)
      .config("spark.shuffle.spill", true)
//      .config("spark.sql.shuffle.partitions", shufflePartitions)
//      .config("spark.memory.fraction", 0.8)
//      .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 720)
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.spill.numElementsForceSpillThreshold", 1024 * 1024 * 1024 / 8)
      //.config("spark.sql.windowExec.buffer.spill.threshold", 20 * 1024 * 1024)
      //.master("spark://master1:17077")
      .getOrCreate()

    val source = Source.fromFile(inputcar,"utf8")
    val lines = source.getLines
    var mapping = Map[String,Int]()
    for(line <- lines){
      val vin = line.split(",")(0)
      val car_type = line.split(",")(1).toInt
      mapping += (vin -> car_type)
    }
    //    println(mapping.mkString)
    source.close

    val spark = sc.newSession()
    import spark.implicits._
    val broadcastVar = sc.sparkContext.broadcast(mapping)
    spark.udf.register("car_type", (vin: String) =>{
      var v = 2
      if(broadcastVar.value.contains(vin)) {
        v = broadcastVar.value(vin)
      }
      v
    })

//    val winSpec1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
//    val winSpec2 = Window.partitionBy($"vin",$"seq_id").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
//    val winSpec3 = Window.partitionBy($"vin",$"seq_id").orderBy($"ts").rowsBetween(Window.currentRow, Window.unboundedFollowing)

    val orig = spark.read.parquet(inputorig).filter($"bcm_keyst".equalTo(2)).select($"vin", $"ts",$"bcm_keyst",$"bcs_vehspd",$"ems_engspd")
      .withColumn("car_type", callUDF("car_type",$"vin"))
      .filter($"car_type".notEqual(2) && $"year".equalTo(year) && $"month".equalTo(month) && ($"ccs_chargerstartst".equalTo(1) || ($"ccs_chargerstartst".isNull)))


    //all
    val mix = orig
    val mix_run = mix.count()
    val mix_idle = mix.filter($"bcs_vehspd".equalTo(0)).count()
    val mix_idle_pure = mix.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".equalTo(0) ).count
    val mix_idle_hybrid = mix.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".notEqual(0) ).count

    //private
    val run_private = orig.filter($"car_type".equalTo(0))
    val pri_run = run_private.count()
    val pri_idle = run_private.filter($"bcs_vehspd".equalTo(0)).count()
    val pri_idle_pure = run_private.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".equalTo(0) ).count
    val pri_idle_hybrid = run_private.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".notEqual(0) ).count


    //taxi
    val run_taxi = orig.filter($"car_type".equalTo(1))
    val tax_run = run_taxi.count()
    val tax_idle = run_taxi.filter($"bcs_vehspd".equalTo(0)).count()
    val tax_idle_pure = run_taxi.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".equalTo(0) ).count
    val tax_idle_hybrid = run_taxi.filter($"bcs_vehspd".equalTo(0) && $"ems_engspd".notEqual(0) ).count



    println("mix_run,"+ mix_run)
    println("mix_idle,"+ mix_idle)
    println("mix_idle_pure,"+ mix_idle_pure)
    println("mix_idle_hybrid,"+ mix_idle_hybrid)


    println("-------------")
    println("pri_run,"+ pri_run)
    println("pri_idle,"+ pri_idle)
    println("pri_idle_pure,"+ pri_idle_pure)
    println("pri_idle_hybrid,"+ pri_idle_hybrid)

    println("-------------")

    println("tax_run,"+ tax_run)
    println("tax_idle,"+ tax_idle)
    println("tax_idle_pure,"+ tax_idle_pure)
    println("tax_idle_hybrid,"+ tax_idle_hybrid)

  }

}
