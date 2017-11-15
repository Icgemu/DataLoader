package cn.gaei.ev

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.io.Source

object CarIdelSpeedWin {

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

    spark.udf.register("t_state", (e: Seq[Long]) =>{
      var v = 0
      if(e.size < 2) {
        v = 1
      }else{
        val t1 = e(0)
        val t2 = e(1)
        if(t2 - t1 > 20*1000){
          v = 1
        }
      }
      v
    })

    spark.udf.register("state", (e: Seq[Seq[Long]]) => {
      var res = 1
      if (e.size == 2) {
        val a1 = e(0)(0)
        val t1 = e(0)(1)
        val a2 = e(1)(0)
        val t2 = e(1)(1)
        if(t2 - t1 <20*1000){
          if((a1 == 0) &&  (a2 == 0)){
            res = 0
          }
        }

      } else if(e.size > 2){
        val a1 = e(0)(0)
        val a2 = e(1)(0)
        val a3 = e(2)(0)
        val t1 = e(0)(1)
        val t2 = e(1)(1)
        val t3 = e(2)(1)

        if(t2-t1 < 20* 1000){
          if (a1 == 0 && a2 == 0) {
            res = 0
          }
        }

        if(t3-t2 < 20* 1000){
          if (a2 == 0 && a3 == 0) {
            res = 0
          }
        }
      }
      res
    })

    val winSpec = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, 1)
    val winSpec1 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(-1, Window.currentRow)
    val winSpec2 = Window.partitionBy($"vin").orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winSpec3 = Window.partitionBy($"vin",$"t_state_1")
      .orderBy($"ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val orig = spark.read.parquet(inputorig).filter($"bcm_keyst".equalTo(2))
      .select($"vin", $"ts",$"bcm_keyst",$"bcs_vehspd",$"ems_engspd",$"bms_battsoc")
      .withColumn("car_type", callUDF("car_type",$"vin"))
      .filter($"car_type".notEqual(2))
      .filter($"year".equalTo(year) && $"month".equalTo(month) &&
        ($"ccs_chargerstartst".equalTo(1) || ($"ccs_chargerstartst".isNull)))
      .withColumn("state", when($"bcs_vehspd".equalTo(0), 0).otherwise(1))
    val idle = orig.filter($"state".equalTo(0))
      .withColumn("t_state", callUDF("t_state",collect_list($"ts").over(winSpec1)))
      .withColumn("t_state_1", sum($"t_state").over(winSpec2))
      .withColumn("eng_max", max($"ems_engspd").over(winSpec3))
      .withColumn("eng_min", min($"ems_engspd").over(winSpec3))
//      .groupBy($"vin",$"t_state_1").agg(max($"ems_engspd").as("eng_max"),min($"ems_engspd").as("eng_min"))
      //.filter($"eng_min".equalTo(0) && $"eng_max".gt(0))
//      .filter($"vin".equalTo("LMGGN1S50F1000916")  && $"ts".gt(1483200794000l-10*60*1000l) && $"ts".lt(1483200794000l+10*60*1000l) ).sort($"ts")
      //.show(100)
      //.withColumn("fre_state", callUDF("state",collect_list(array($"state",$"ts")).over(winSpec)))
      .cache()
//    orig.filter($"ems_engspd".gt(0)).agg(avg($"bms_battsoc").as("avg"),min($"bms_battsoc").as("min"),max($"bms_battsoc").as("max")).collect().foreach(e => println(e.mkString(",")))
    //all
    val mix = orig
    val mix_run = mix.count()
//    val mix_run_idle = mix.withColumn("state", when($"bcs_vehspd".equalTo(0), 0).otherwise(1))
//    mix_run_idle.withColumn("fre_state", callUDF("state",collect_list($"flags").over(winSpec)))
    val mix_idle = idle.count()
    val mix_idle_pure = idle.filter($"eng_max".equalTo(0) && $"eng_min".equalTo(0) ).count
    val mix_idle_hybrid = idle.filter($"eng_min".geq(0) && $"eng_max".notEqual(0) ).count

    //private
    val run_private = idle.filter($"car_type".equalTo(0))
    val pri_run = orig.filter($"car_type".equalTo(0)).count()
//    val pri_run_idle = mix.withColumn("state", when($"bcs_vehspd".equalTo(0), 0).otherwise(1))
//    pri_run_idle.withColumn("fre_state", callUDF("state",collect_list($"flags").over(winSpec)))
    val pri_idle = run_private.count()
    val pri_idle_pure = run_private.filter($"eng_max".equalTo(0) && $"eng_min".equalTo(0) ).count
    val pri_idle_hybrid = run_private.filter($"eng_min".geq(0) && $"eng_max".notEqual(0) ).count


    //taxi
    val run_taxi = idle.filter($"car_type".equalTo(1))
    val tax_run = orig.filter($"car_type".equalTo(1)).count()
//    val tax_run_idle = mix.withColumn("state", when($"bcs_vehspd".equalTo(0), 0).otherwise(1))
//    tax_run_idle.withColumn("fre_state", callUDF("state",collect_list($"flags").over(winSpec)))
    val tax_idle = run_taxi.count()
    val tax_idle_pure = run_taxi.filter($"eng_max".equalTo(0) && $"eng_min".equalTo(0)  ).count
    val tax_idle_hybrid = run_taxi.filter($"eng_min".geq(0) && $"eng_max".notEqual(0) ).count



//    val writer = new PrintWriter("LMGGN1S50F1000916.csv")
//    orig
////      .filter($"vin".equalTo("LMGGN1S52E1000138"))
////      .select($"vin", $"car_type", from_unixtime($"ts" / 1000, "yyyy-MM-dd HH:mm:ss"), $"bms_battsoc", $"bcs_vehspd", $"ems_engspd", $"state", $"fre_state")
//      .collect().foreach(e =>writer.write(e.mkString(",")+"\n"))
//    writer.close()

//    orig.filter($"ems_engspd".gt(0))
//      .agg(avg($"bms_battsoc").as("avg"),min($"bms_battsoc").as("min"),max($"bms_battsoc").as("max"))
//      .collect().foreach(e => println(e.mkString(",")))

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
