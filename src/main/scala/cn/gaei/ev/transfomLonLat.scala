package cn.gaei.ev

import java.io.File
import java.nio.charset.Charset

import cn.gaei.ev.MapGeoHashToCityID.{subDir}
import com.google.common.io.Files
import scala.io.Source

/**
  * Created by eshgfuu on 2017/8/7.
  */
object transfomLonLat {
  def main(args: Array[String]): Unit = {
    for(d <- subDir(new File("./bound/china"))){

      val city = d.getName.replace(".txt","")
      val newFile = d.getParentFile
      val f = new File(newFile,city+"-1.txt")
      if(f.createNewFile()){
        val w = Files.newWriter(f, Charset.forName("UTF-8"))
        val source = Source.fromFile(d)
        val str = source.mkString.split(";")

        str.foreach(e=>{
          w.write(e)
          w.newLine()
        })
        w.close()
      }
    }
  }
}
