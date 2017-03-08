package parser

import scala.util.matching.Regex
import java.util.regex.Pattern
import java.util.regex.Matcher

/**
  * Created by hungdv on 07/03/2017.
  * For testing pupose
  */
object RegextTest {
 /*   def main(args: Array[String]):Unit = {
      //val logExample = "\"Feb 01 2017 06:59:59\",\"BDH-MP01-4\""
      val logExample = "\"BDH-MP01-4\""
      //val logExample = "\"ACTALIVE\",\"Feb 01 2017 06:59:59\",\"QNI-MP01-2\",\"-176151750\",\"Qidsl-130927-862\",\"604409\",\"2077300680\",\"-1153734931\",\"0\",\"825597\",\"100.106.152.72\",\"bc:96:80:16:ba:44\",\"\""
      val dateTimePattern = "(\"\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2}\")"


      val firstQuote = "(\")"
      //val delemiter = "(\",\")"
      val delemiter = "([,])"
      val lastQuote ="(\")"
      //val NASName = "(\"[a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,}\")"
      //val NASName = "(\"[a-z]{2,}\\d{1,}\"|\"\\w{1,}-\\w{1,}-\\w{1,}\"|\"\\w{1,}-\\w{1,}\"|\"\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,}\")"
      //val lineMatch = s"""$dateTimePattern""".r
      //val lineMatch: Regex = s"$firstQuote$dateTimePattern$delemiter$NASName$lastQuote".r

      val lineMatch: Regex = s"$dateTimePattern$delemiter$NASName".r

      //val p = Pattern.compile(lineMatch)
      logExample match {
      case lineMatch(dateTimePattern,delemiter,NASName) => println("True")
      case  _ => println("False")
          /*
             Cach 1 : Dung regex : s""
             Cach 2: Dung Pattern trong java.util.regex.Pattern.
             Cach 1: buoc phai co ()
             Ref : https://www.tutorialspoint.com/scala/scala_regular_expressions.htm
           */
     }
    }*/


/*  def main(args: Array[String]):Unit = {
    //val logExample = "\"Feb 01 2017 06:59:59\",\"BDH-MP01-4\""
    //val dateTimeLogExample = "\"Feb 01 2017 06:59:59\""
    //val logExample = "\"BDH-MP01-4\""

    //val dateTimePattern = "(\"\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2}\")" //True
    //val dateTimePattern = "\"(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})\""
    //val delemiter = "(\",\")"
    val delemiter = "([,])"
    val logExample = "BDH-MP01-4"
    //val NASName = "\"(w{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})\""
    //val NASName ="\"(\\w{1,}-\\w{1,}-\\w{1,})\"|([^ ]*)"
    val NASName = "([^ ]*)"
    //val NASName = "(\"[a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,}\")"
    //val NASName = "(\"[a-z]{2,}\\d{1,}\"|\"\\w{1,}-\\w{1,}-\\w{1,}\"|\"\\w{1,}-\\w{1,}\"|\"\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,}\")"
    //val lineMatch = s"""$dateTimePattern""".r
    //val lineMatch: Regex = s"$firstQuote$dateTimePattern$delemiter$NASName$lastQuote".r
    val lineMatch = s"$NASName".r
    //val dateTimeLineMatcher  = s"$dateTimePattern".r

    //val lineMatch: Regex = s"$dateTimePattern$delemiter$NASName".r

    //val p = Pattern.compile(lineMatch)
    logExample match {
      case lineMatch(NASName) => println("True - NASName")
      case  _ => println("False - NASName")
      /*
         Cach 1 : Dung regex : s""
         Cach 2: Dung Pattern trong java.util.regex.Pattern.
         Cach 1: buoc phai co ()
         Ref : https://www.tutorialspoint.com/scala/scala_regular_expressions.htm
       */
    }

  /*  dateTimeLogExample match{
      case dateTimeLineMatcher(dateTimePattern) => println("True - Datetime")
      case _ => println("False - DateTime")
    }*/

  }*/


  def main(args: Array[String]): Unit={
    //////////////////////////////////////////////
//    val NASNameLogExample :String= "aaaa"
//    //val NASName2 = "([a-z]{4,})"
//    //val NASName2  = "(\\w{4,})"
//    val NASName2 =  "([^\\s]*)"
//    //val NASName2 =  "([^ ]*)"
    /////////////////////////////////////////////

    //val NASNameLogExample = "111 aa"
    //val NasNamePattern = "(\\d{1,})"
    //val NasNamePattern = "(\\w{1,}\\s+\\w{1,})" //Faslse
    //val NasNamePattern = "(\\w{1,} \\w{1,})" //False
    //val NasNamePattern  = "([^ ]*) ([^ ]*)" // False

    //val NasNameLogExample = "scala"
    //val NasNamePattern  =   "\\w{5}"

    /*val log =   "Feb 01 2017 06:59:59"
    val pattern  = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"

    val regex  = s"$pattern".r
    log match{
      case regex(pattern) => println("True - NASName")
      case _ => println(("False - NASName"))
    }*/


   /* val aaaa :String= "aaaa"
    val patternAAA = "([a-z]{4,})"
    val matcherAAA = s"$patternAAA".r
    aaaa match{
      case matcherAAA(patternAAA) => println("Mother Fucking True")
      case _ => println("Ton of shit")
    }*/
    val test = "\"Feb 01 2017 06:59:59\",\"BDH-MP01-4\""

    val dateTimePattern = "\"(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})\""
    val nasNamePattern  = "\"([a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})\""
                            "([a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})"
    val regex = s"$dateTimePattern,$nasNamePattern".r

    test match{
      case regex(dateTimePattern,nasNamePattern) => println("true")
      case _ => println("false")
    }

    val dateTimePatternWithOutQuote = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"
    val nasNamePatternWithOutQuote  = "([a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})"
                                    //"([a-z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})"



 /*   // JAVA Style
    val javaRegex = Pattern.compile(regex.toString())
    val matcher = javaRegex.matcher(test)
    if(matcher.find()) {
      buildLogObject(matcher)
    }else {
      None
    }*/
     val time1         = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"
    val time2                 = "(\\d{2}:\\d{2}:\\d{2})"
    val ssThreadId            = "(\\w{8,}|\\w+[.]\\w+[.]?\\w+)"
    val unindentified         = "(\\w{4,})"
    val sessID                = "([^\"]+)"
    val conTask               = "(\\D{4,}):(\\D{6,}):"
     val status        = "([a-zA-Z]{6,})"
    val NASName               = "([a-zA-Z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})"
     val realNumber    = "(-?\\d{1,})"
     val ip            = "(\\d{1,}[.]\\d{1,}[.]\\d{1,}[.]\\d{1,})"
     val mac           = "(\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2})"
     val text          = "(.*)"
    val conName               = "([a-zA-Z]{1,}\\d?[-_.]?\\w{3,}[-_.]{1,}?\\d{1,}|" +
      "[a-zA-Z]{3,}[-_]\\d{3,}-?|opennet|-\\D{1}|" +
      "[a-zA-Z]{3,}-iptv\\d{1,}|ut kho|fpt|[(]null[)]|"+
      "[a-zA-Z]{2,}-\\w{3,}|" +
      "[a-zA-Z]{5}-[a-zA-Z]{1,}-[a-zA-Z]{1,}|" +
      "[a-zA-Z]{5}-[a-zA-Z]{1,}|" +
      "\\w{2,}|\\w{1,}@\\D+|" +
      "\\w{1,}[.][a-z]{3}|" +
      "[a-zA-Z]{4,}-\\d{1,}-\\d{1,}-\\d{1,}|" +
      "\\w{5,}-\\d{1,}-\\d{1,}|" +
      "\\W?\\w{1,}\\W?\\w{1,}-\\w{1,}-\\w{1,}|[a-zA-Z\\s]+|" +
      "[a-zA-Z]{1,}[-_./&]+\\d+[-_.]+\\d+|" +
      "\\d+[.]\\d+[.]\\d+[.]\\d+|" +
      "\\w+[-/]+\\d+[-]\\d+|" +
      "[a-zA-Z1-9._@&#;]+)[.]*"

    def addQuote(string:  String): String ={
      return "\"" +string+ "\""
    }
    val time1WithQuote = addQuote(time1)
    val NASNameWithQuote = addQuote(NASName)
    val realNumberWithQuote = addQuote(realNumber)
    val conNameWithQuote  = addQuote(conName)
    val sessIDWithQuote  =  addQuote(sessID)
    val ipWithQuote = addQuote(ip)
    val macWithQuote  = addQuote(mac)
    val statusWithQuote = addQuote(status)
    val textWithQuote   = addQuote(text)

    val loadLogExample1 = "\"ACTALIVE\",\"Feb 01 2017 06:59:59\",\"BDH-MP01-4\",\"-1015788159\",\"Bidsl-131023-034\",\"661208\",\"1441294524\",\"-1442593265\",\"0\",\"674988\",\"100.110.230.122\",\"64:d9:54:82:f2:f4\",\"\""

    val statusExample = "\"ACTALIVE\""
    val timeExample = "\"Feb 01 2017 06:59:59\""
    val NASNameExample = "\"BDH-MP01-4\""
    val realNumberExample = "\"-1015788159\""
    val conNameExample  = "\"Bidsl-131023-034\""
    val sessIDExample = ""
    val ipExample = "\"100.110.230.122\""
    val macExample  = "\"64:d9:54:82:f2:f4\""
    val textExample = "\"2405:4800:4146:8808:0000:0000:0000:0000/64\""
    val nullTextExample = "\"\""

    val statusRegex = s"$statusWithQuote".r
    val time1Regex  = s"$time1WithQuote".r
    val NASNameRegex  = s"$NASNameWithQuote".r
    val realNumberRegex =s"$realNumberWithQuote".r
    val conNameRegex  = s"$conNameWithQuote".r
    val sessIDRegex   = s"$sessIDWithQuote".r
    val ipRegex  = s"$ipWithQuote".r
    val macRegex  = s"$macWithQuote".r

    val textRegex = s"$textWithQuote".r


    statusExample match{
      case statusRegex(statusWithQuote) => println("true status")
      case _ => println("false status")
    }

    timeExample match{
      case time1Regex(time1WithQuote) => println("true time1")
      case _ => println("false time1")
    }

    NASNameExample match{
      case NASNameRegex(NASNameWithQuote) => println("true NASNameWithQuote")
      case _ => println("false NASNameWithQuote")
    }

    realNumberExample match{
      case realNumberRegex(realNumberWithQuote) => println("true realNumberWithQuote")
      case _ => println("false realNumberWithQuote")
    }
    textExample match{
      case textRegex(textWithQuote) => println("true textWithQuote")
      case _ => println("false textWithQuote")
    }

    nullTextExample match{
      case textRegex(textWithQuote) => println("true nullTextExample")
      case _ => println("false nullTextExample")
    }




 /*   def buildLogObject(matcher: Matcher) = {
      LogObject(
        matcher.group(1),
        matcher.group(2)
      )
    }
    def parseRecord(record: String): Option[LogObject]={
      val regex = s"$dateTimePattern,$nasNamePattern".r
      // JAVA Style
      val javaRegex = Pattern.compile(regex.toString())
      val matcher = javaRegex.matcher(test)
      if(matcher.find()) {
        Some(buildLogObject(matcher))
      }else {
        None
      }
    }
    def parseRecordReturningNullObjectOnFailure(record: String): LogObject={
      val regex = s"$dateTimePattern,$nasNamePattern".r
      // JAVA Style
      val javaRegex = Pattern.compile(regex.toString())
      val matcher = javaRegex.matcher(test)
      if(matcher.find()) {
        buildLogObject(matcher)
      }else {
        LogObject.nullLogObject
      }
    }*/

  }

/*def main(args: Array[String]): Unit={

  val dateTimeLogExample = "Feb 01 2017 06:59:59"
  val dateTimePattern = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"


  val dateTimeMatcher  = s"$dateTimePattern".r
  dateTimeLogExample match{
    case dateTimeMatcher(dateTimePattern) => println("True - NASName")
    case _ => println(("False - NASName"))
  }
}*/



}
case class LogObject(dateTime: String, NASName: String){

}
object LogObject{
  val nullLogObject = LogObject("","")
}