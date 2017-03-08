package parser
import parser.LoadLogParser

/**
  * Created by hungdv on 08/03/2017.
  */
object LoadLogParserTest {
  def main(args: Array[String]): Unit ={
    val loadLogExample1 = "\"ACTALIVE\",\"Feb 01 2017 06:59:59\",\"BDH-MP01-4\",\"-1015788159\",\"Bidsl-131023-034\",\"661208\",\"1441294524\",\"-1442593265\",\"0\",\"674988\",\"100.110.230.122\",\"64:d9:54:82:f2:f4\",\"\""
    val loadLogExample2 = "\"ACTALIVE\",\"Feb 01 2017 06:59:59\",\"HUE-MP01-1-NEW\",\"1073743052\",\"Hufdl-160614-886\",\"9870720\",\"1222357540\",\"-1631997462\",\"0\",\"237580\",\"100.104.160.110\",\"4c:f2:bf:42:71:4a\",\"2405:4800:4146:8808:0000:0000:0000:0000/64\""
    val loadLogExample3 = "\"Feb 01 2017 06:59:59\",\"HUE-MP01-1-NEW\",\"1073743052\",\"Hufdl-160614-886\",\"9870720\",\"1222357540\",\"-1631997462\",\"0\",\"237580\",\"100.104.160.110\",\"4c:f2:bf:42:71:4a\",\"2405:4800:4146:8808:0000:0000:0000:0000/64\""

    val loadLogPaser = new LoadLogParser()
    val line1 = loadLogPaser.extractValues(loadLogExample1).get
    println("line1 : " + line1.toString)

    val line2 = loadLogPaser.extractValues(loadLogExample2).get
    println("line1 : " + line2.toString)
    val line3 = loadLogPaser.extractValues(loadLogExample3).get
    println("line1 : " + line3.toString)
  }
}
