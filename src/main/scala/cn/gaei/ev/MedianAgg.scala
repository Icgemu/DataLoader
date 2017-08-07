package cn.gaei.ev

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gz02559 on 2017/8/7.
  */
//case class DataFrames(vin:String,date_str:String,bms_batttempmax: Double,city:String)
case class Median(var sum: ArrayBuffer[Double], var count: Long)

object MedianAgg extends Aggregator[Row, Median, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Median = Median(new ArrayBuffer(), 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Median, employee: Row): Median = {
    buffer.sum += employee.getDouble(2)
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Median, b2: Median): Median = {
    b1.sum ++= b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Median): Double = {
    val all = reduction.sum.toList.sorted
    if(reduction.count % 2 != 0){
      all((reduction.count / 2).toInt)
    }else{
      val a = ((reduction.count-1) / 2).toInt
      val b = a+1
      (all(b) + all(a))/2
    }

  }
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Median] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}