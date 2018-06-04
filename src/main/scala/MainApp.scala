import java.nio.file.{Files, Paths}

import util.InitXMLData

import scala.sys._
import scala.util.control.Breaks.break

object MainApp {

  def main(args: Array[String]): Unit = {

    // Check program arguments block
    if (args.length != 1) {
      println(
        "Error: Cannot find program arguments.\n" +
          "Usage: spark-submit --master <server-name> --class MainApp ./smms.jar <config.xml>"
      )
      exit(-1)
    }

    if (!checkFile(args(0))) {
      println(s"Cannot read config file: $args(0)")
      exit(-1)

    }

    // Start processing
    val initXMLData = new InitXMLData()
    val propMap: Map[String, String] = initXMLData.initGeneralAndFsData(args(0))
    val stgMap: Map[String, Boolean] = initXMLData.initStgMapAdv(args(0))
    val sparkMap: Map[String, String] = initXMLData.initSparkMap(args(0))

    println("Initial running ...")
    new StgInit(propMap, sparkMap, stgMap)

  }

  def checkFile(fn: String): Boolean = Files.exists(Paths.get(fn)) && Files.isReadable(Paths.get(fn))

}


