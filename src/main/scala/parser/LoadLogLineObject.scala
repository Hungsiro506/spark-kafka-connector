package parser

/**
  * Created by hungdv on 06/03/2017.
  */
/**
  * Treat All As String.
  * @param actStatus : String ActStatus (ex: ACTALIVE)
  * @param date : datetime  Date  (ex:Feb 01 2017 20:47:06)
  * @param NASName  : String
  * @param NASPort  : String
  * @param name : String
  * @param sessID : String
  * @param input  : int
  * @param output : int
  * @param termCode : String
  * @param sessTime : int
  * @param IPAdress : String
  * @param callerID : String
  * @param rest: String
  */
case class LoadLogLineObject(
    actStatus:  String,
    date:       String,
    NASName:    String,
    NASPort:    String,
    name:       String,
    sessID:     String,
    input:      String,
    output:     String,
    termCode:   String,
    sessTime:   String,
    IPAdress:   String,
    callerID:   String,
    rest:       String
)extends AbtractLogLine

object LoadLogLineObject{
  val nullUsageLogLine = LoadLogLineObject("","","","","","","","","","","","","")
}
