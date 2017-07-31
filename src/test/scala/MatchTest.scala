/**
  * Created by pc-admin on 2017/7/19.
  */
object MatchTest {
  def main(args: Array[String]): Unit = {
    val content = "name=fred,age=23"
    val regx = "(?<=name=)[a-zA-Z]+|(?<=age=)[0-9]+"
    regx.r.findAllIn(content).toList.foreach(println(_))
  }

}
