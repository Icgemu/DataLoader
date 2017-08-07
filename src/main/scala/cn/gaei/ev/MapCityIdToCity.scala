package cn.gaei.ev

import java.io.PrintWriter

import ch.hsr.geohash.GeoHash
import cn.gaei.ev.MapGeoHashToCityID.{geometryFactory, mapping}
import com.vividsolutions.jts.geom.Coordinate

import scala.collection.mutable
import scala.io.Source

/**
  * Created by gz02559 on 2017/8/7.
  */
object MapCityIdToCity {

  def main(args: Array[String]): Unit = {
    var source = Source.fromFile("id-to-city.csv")
    var lines = source.getLines
    val mapping = new mutable.HashMap[String,String]()
    for(line <- lines){
      val id = line.split(",")(0)
      val city = line.split(",")(1)

      mapping.put(id,city)
    }
    source.close


    source = Source.fromFile("geo.csv")
    lines = source.getLines
    val writer = new PrintWriter("geo-mapped.csv")
    writer.println("name,id,date_str,maxTemp,avgTemp,medianTemp,cnt")
    for(line <- lines){
      val id = line.split(",")(0)
      val city = mapping.get(id).getOrElse("未知")
      writer.println(city +","+  line)
    }
    writer.close()
    source.close
  }

}
