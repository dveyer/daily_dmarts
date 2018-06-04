package util

import scala.xml.XML

class InitXMLData {

  def initGeneralAndFsData(fn: String):Map[String, String]={
    var genMap:Map[String, String] = Map.empty[String, String]
    val xml = XML.loadFile(fn)
    // **** Init General data ****
    val genNodes = (xml\\"init"\\"general").iterator
    while(genNodes.hasNext) {
      val node = genNodes.next()
      for(ss <- node.child) genMap += (ss.label -> ss.text)
    }
    // **** Init FS data ****
    val fsNodes = (xml\\"init"\\"fs").iterator
    while(fsNodes.hasNext) {
      val node = fsNodes.next()
      for(ss <- node.child) genMap += (ss.label -> ss.text)
    }
    genMap
  }

  def initStgMapAdv(fn:String):Map[String, Boolean]={
    var stgMap:Map[String, Boolean] = Map.empty[String, Boolean]
    val xml = XML.loadFile(fn)
    val stgNodes = (xml\\"stgmap"\\"stg").iterator
    while(stgNodes.hasNext) {
      val stgNode = stgNodes.next()
      val enableStatus = (stgNode \ "id" \ "@status").text.equals("1")
      val period = (stgNode \ "id" \ "@period").text
      val pdate = (stgNode \ "id" \ "@pdate").text
      val scale = (stgNode \ "id" \ "@scale").text
      val level = (stgNode \ "id" \ "@level").text
      stgMap += ((stgNode \ "id").text+"|"+period+"|"+pdate+"|"+scale+"|"+level -> enableStatus)
    }
    stgMap
  }

  def initSparkMap(fn: String):Map[String, String]={
    var sparkMap:Map[String, String] = Map.empty[String, String]
    val xml = XML.loadFile(fn)
    // **** Init Spark data ****
    val genNodes = (xml\\"init"\\"spark").iterator
    while(genNodes.hasNext) {
      val node = genNodes.next()
      for(ss <- node.child) sparkMap += (ss.label -> ss.text)
    }
    sparkMap
  }

}
