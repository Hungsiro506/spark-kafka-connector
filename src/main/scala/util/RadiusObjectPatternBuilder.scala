package util
import scala.util.matching.Regex
/**
  * Created by hungdv on 06/03/2017.
  */
class RadiusObjectPatternBuilder {

}
object AtomicPattern {
    private val time1         = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"
    val time2                 = "(\\d{2}:\\d{2}:\\d{2})"
    val ssThreadId            = "(\\w{8,}|\\w+[.]\\w+[.]?\\w+)"
    val unindentified         = "(\\w{4,})"
    val sessID                = "([^\"]+)"
    //val conTask               = "(\\D{4,}):(\\D{6,}):"
    val conTask               = "(\\D{4,}:\\D{6,}):"
    private val status        = "([a-zA-Z]{6,})"
    val nASName               = "([a-zA-Z]{2,}\\d{1,}|\\w{1,}-\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}|\\w{1,}-\\w{1,}-\\w{1,}-\\w{1,})"
    private val realNumber    = "(-?\\d{1,})"
    private val ip            = "(\\d{1,}[.]\\d{1,}[.]\\d{1,}[.]\\d{1,})"
    private val mac           = "(\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2})"
    private val text          = "(.*)"
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

     val rejectCause        = text
     val rejectResultDetail = text
  /**
    * Convert from python code @autho - Annt
    */
  private var conRes = Map(
        "Content"           -> text,
        "LForce"            -> (conName + ", " + text),
        "SignIn"            -> (conName + ", " + nASName + ", " + unindentified),
        "LogOff"            -> (conName + ", " + nASName + ", " + unindentified),
        "Reject"            -> (conName + ", " + rejectCause + ", " + rejectResultDetail),
        "Silent Discard"    -> ("[[]" + conName + ", " + mac + "[]]"),
        "Invalid Radius Packet from"-> (ip + text)
  )
  val passlib = Array("error", "Consultez les messages SQL Server", "Unauthorized to monitor",
    "UNAUTHORIZED MONITOR CONNECTION",  "dberror", "ॅ牲敵爠柩滩牡汥⁳畲⁓兌⁓敲癥爠㨠䍯湳畬瑥稠汥猠浥獳慧敳⁓兌⁓敲癥爮",
    "dbAllocate:Auth:To many DBConn", "InternetGlobal..PR_XDSL_Accounting", "Internet..PR_XDSL_Accounting",
    "InternetGlobal..PR_XDSL_Authorize", "Internet..PR_XDSL_Authorize",
    "InternetGlobal..PR_XDSL_Interim", "Internet..PR_XDSL_Interim", "InternetGlobal..PR_XDSL_Logon", "Internet..PR_XDSL_Logon",
    "UnAuthorized Radius Client", "acct:recvfrom", "auth:recvfrom", "Socket",
    "Power Radius", "Local Address", "Can not connect to Database Server", "CurrentDB", "nombre maximal de DBPROCESS",
    "To many DBConn", "Over Session Limit, Bypass", "Invalid Radius Packet",
    "SGdsl-noc-radius", "ACCEPT MONITOR CONNECTION", "DISCONNECT WITH", "PowerRadius..QueryRulePRO: Error Executed")

  val ignore_list = Array("Invalid User Name", "Invalid UsrName", "Auth-Local:Reject: , Unknown Error",
    "PFSENSE", ".localdomain", "dorm.f.fu")
  private val comma = ","
  private val whiteSpacePattern = "\\s+"
  private val whileSpaceHardcode = " "
  private val delimiter = "\",\""


  def addQuote(string:  String): String ={
    return "\"" +string+ "\""
  }
   val time1WithQuote = addQuote(time1)
   val nASNameWithQuote = addQuote(nASName)
   val realNumberWithQuote = addQuote(realNumber)
   val conNameWithQuote  = addQuote(conName)
   val sessIDWithQuote  =  addQuote(sessID)
   val ipWithQuote = addQuote(ip)
   val macWithQuote  = addQuote(mac)
   val statusWithQuote = addQuote(status)
   val textWithQuote   = addQuote(text)


  // Shoud not put this thing here, put in specific parser.
  // pattern without Quote sign
   // old devide doesn't hvae status
  val loadPattern = addQuote(time1)+comma+addQuote(nASName)+comma+addQuote(realNumber)+comma+addQuote(conName)+
  comma+addQuote(sessID)+comma+addQuote(realNumber)+comma+addQuote(realNumber)+comma+addQuote(realNumber)+comma+
    addQuote(realNumber)+comma+addQuote(ip)+comma+addQuote(mac)+comma+addQuote(text)
  // new devide has one more field status.
  val loadStatusPattern = addQuote(status)+comma+loadPattern
  val content = conRes("Content")
  val conPattern =  time2+whileSpaceHardcode+ssThreadId+whileSpaceHardcode+conTask+whileSpaceHardcode+content
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // Patterns with Quote sign
  val loadRegexPattern: Regex   = s"$time1WithQuote,$nASNameWithQuote,$realNumberWithQuote,$conNameWithQuote,$sessIDWithQuote,$realNumberWithQuote,$realNumberWithQuote,$realNumberWithQuote,$realNumberWithQuote,$ipWithQuote,$macWithQuote,$textWithQuote".r
  val loadStatusRegexPattern: Regex   = new Regex(s"$statusWithQuote" +","+ s"$loadRegexPattern")
  val conRegexPattern: Regex =   s"$time2\\s+$ssThreadId\\s+$conTask\\s+$content".r

  val signInLogOffRegexPattern: Regex = s"$time2\\s+$ssThreadId\\s+$conTask\\s+$conName,\\s+$nASName,\\s+$text".r
  val rejectRegexPattern: Regex       = s"$time2\\s+$ssThreadId\\s+$conTask\\s+$conName,\\s+$rejectCause,\\s+$rejectResultDetail".r

  //Define head and tail of load - conn log for classifier
  val loadHead: String = "\"(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2}|[a-zA-Z]{6,})\""  // time1 | status
  val loadTail = "(.*)" // Match any cheracter except new line.
  val loadGeneralRegex = s"$loadHead,$loadTail".r

  val connHead: String = time2
  val connTail: String = "(.*)"
  val connGeneralRegex = s"$connHead\\s+$connTail".r



}
