package launch


import scala.io.Source

object broadCastProcess {
  def getProvinceData(path: String) = {
    var map = Map[String, String]()
    val lines = Source.fromFile(path).getLines()
    lines.foreach(line => {
      val code = line.split("\t")
      map += (code(0) -> code(1))
    })
    map
  }

}
