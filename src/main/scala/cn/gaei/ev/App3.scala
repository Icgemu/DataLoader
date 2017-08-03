package cn.gaei.ev
import org.apache.spark.sql.{ SparkSession}
/**
  * @author ${user.name}
  */
object App3 {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.executor.memory", "4G")
      .config("spark.executor.cores", "2")
      //.master("spark://fushengrongdeMacBook-Pro.local:7077")
      .master("spark://master1:17077")
      .getOrCreate()

    val spark = sc.newSession()
    import spark.implicits._
    var file = spark.read.parquet("/data/parquet")
    file = file.filter($"lon02".gt(0) && $"bms_batttempmax".isNotNull).select($"vin",$"ts",$"bms_batttempmax",$"lon02",$"lat02")
    file.groupBy($"vin",$"")
  }
}
