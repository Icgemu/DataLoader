package cn.gaei.ev

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gz02559 on 2017/8/10.
  */
object UntypedMedianAgg extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("in", DoubleType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("sum", ArrayType(DoubleType)) :: StructField("count", LongType) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)= new ArrayBuffer[Double]()
    buffer(1) = 0l
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Seq[Double]](0) :+ input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Seq[Double]](0) :+ buffer2.getAs[Seq[Double]](0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = {
    val all = buffer.getAs[ArrayBuffer[Double]](0).sorted
    val count = buffer.getLong(1)
    if(count % 2 != 0){
      all((count / 2).toInt)
    }else{
      val a = ((count-1) / 2).toInt
      val b = a+1
      (all(b) + all(a))/2
    }
  }
}
