import utl.Tools

class StgInit(propMap: Map[String, String], sparkMap: Map[String, String],stgMap:Map[String, Boolean]) {

  val tools = new Tools("Asia/Almaty", "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", propMap)
  var stg_status:Map[String, Boolean] = Map.empty[String, Boolean]

  for((stg,runStatus) <- stgMap) {

    val m: Int = stg.split("\\|")(1).toInt
    val pdate = stg.split("\\|")(2)
    val scale = stg.split("\\|")(3)

    if (scale.equals("daily") && runStatus) {

      val stgClass = Class.forName(stg.split("\\|")(0))
      val stgConstructor = stgClass.getConstructor(
        classOf[scala.collection.immutable.Map[String, String]],
        classOf[scala.collection.immutable.Map[String, String]],
        classOf[String],
        tools.getClass
      )
      println("Running: " + stg.split("\\|")(0))
      val stgRunJob = stgConstructor.newInstance(propMap, sparkMap, pdate, tools).getClass.getMethod("RunJob")
      stg_status += (stg.split("\\|")(0) ->
        stgRunJob
          .invoke(stgConstructor.newInstance(propMap, sparkMap, pdate, tools))
          .asInstanceOf[Boolean])
    }
  }
  // Check stagings result
  var stgRunStatus = true
  for ((stg, status) <- stg_status)
    if (!status) {
      println(s"WARN $stg was not finished successfully")
      stgRunStatus = false
    }

  if (!stgRunStatus) println("ERROR Stagings calculation were broken")
  else println("INFO Everything is OK")

}
