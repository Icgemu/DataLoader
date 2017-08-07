package cn.gaei.ev

import java.io.{File, PrintWriter}

import ch.hsr.geohash.GeoHash

import scala.collection.JavaConverters._
import scala.io.Source
import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.JTSFactoryFinder
/**
  * Created by gz02559 on 2017/8/4.
  */
object MapGeoHashToCityID {
  val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
  var mapping = Map[Int,Geometry]()
  def main(args: Array[String]): Unit = {
    var i = 1;
    val id_to_map = new PrintWriter("id-to-city.csv")
    for(d <- subDir(new File("D:\\map\\bound\\china"))){
      geoRead(d, i)
      val province = d.getParentFile.getName
      val city = d.getName.replace(".txt","")
      id_to_map.println(i +","+province +"-"+city)
       i+=1
    }
    id_to_map.close()

    val source = Source.fromFile("geo.csv")
    val lines = source.getLines
    val writer = new PrintWriter("geo-map.csv")
    for(line <- lines){
      val code = line.split(",")(0)
      if("NULL"  != code ) {
        val center = GeoHash.fromGeohashString(code).getPoint
        val coord = new Coordinate(center.getLongitude, center.getLatitude);
        val point = geometryFactory.createPoint(coord);

        val c = mapping.filter(e => point.within(e._2))
        c.foreach(e => writer.println(code + "," + e._1))
      }
    }
    writer.close()
    source.close
  }

  def subDir(dir:File): Iterator[File] ={
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }

  def geoRead(f:File, i:Int): Unit ={
    val province = f.getParentFile.getName
//    val city = f.getName.replace(".txt","")
    if("china" != province){

      val source = Source.fromFile(f)
      val str = source.mkString
//      println(str)
      if(!str.trim.isEmpty) {
        val poly = "POLYGON(("+str.replace(","," ").replace(";",",")+"))"
        val reader = new WKTReader(geometryFactory)
        val p = reader.read(poly)
//        println(p.toString)
        mapping += (i -> p)
      }
    }

  }

}
