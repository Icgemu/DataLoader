package cn.gaei.ev
import java.io.PrintWriter

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.Try
/**
  * @author ${user.name}
  */
object GeohashCal {

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile(args(0),"utf8")
    val lines = source.getLines
    var mapping = Map[String,String]()
    for(line <- lines){
      val code = line.split(",")(0)
      val city = line.split(",")(1)
      mapping += (code -> city)
    }
    println(mapping.mkString)
    source.close

    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.executor.memory", "4G")
      .config("spark.executor.cores", "2")
      //.master("spark://fushengrongdeMacBook-Pro.local:7077")
      .master("spark://master1:17077")
      .getOrCreate()
    val broadcastVar = sc.sparkContext.broadcast(mapping)

    val spark = sc.newSession()
    import spark.implicits._
//    val trp = broadcastVar.value

    spark.udf.register("geo", (lon: Double, lat: Double) =>{
      val r = Try(GeoHash.geoHashStringWithCharacterPrecision(lat,lon,5))
      if(r.isSuccess) {
        val v = broadcastVar.value(r.get)
        if(v.)
      } else "NULL"
    })

    val writer = new PrintWriter("geo.csv")
    var file = spark.read.parquet("/data/parquet")
    file = file
      .filter($"lon02".gt(0) && $"bms_batttempmax".isNotNull)
      .select($"vin",$"date_str",$"bms_batttempmax",callUDF("geo",$"lon02",$"lat02").as("city"))
    file
      .groupBy($"city",$"date_str")
      .agg(max($"bms_batttempmax").as("max"), avg($"bms_batttempmax").as("avg"),count("*").as("count"))
      .sort($"city", $"date_str")
      .collect().foreach(e => writer.println(e.mkString(",")))
    writer.close()
  }
}
