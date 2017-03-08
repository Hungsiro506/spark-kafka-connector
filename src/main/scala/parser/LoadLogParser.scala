package parser
import scala.util.matching.Regex
import util.AtomicPattern
import java.util.regex.Pattern
import java.util.regex.Matcher
/**
  * Created by hungdv on 06/03/2017.
  */
class LoadLogParser extends AbtractLogParser{


    private val time1WithQuote      =   AtomicPattern.time1WithQuote
    private val NASNameWithQuote    =   AtomicPattern.NASNameWithQuote
    private val NASPortWithQuote    =   AtomicPattern.realNumberWithQuote
    private val conNameWithQuote    =   AtomicPattern.conNameWithQuote
    private val sessIDWithQuote     =   AtomicPattern.sessIDWithQuote
    private val ipWithQuote         =   AtomicPattern.ipWithQuote
    private val macWithQuote        =   AtomicPattern.macWithQuote
    private val statusWithQuote     =   AtomicPattern.statusWithQuote


    private val input               =   AtomicPattern.realNumberWithQuote
    private val output              =   AtomicPattern.realNumberWithQuote
    private val termcode            =   AtomicPattern.realNumberWithQuote
    private val sessionTime         =   AtomicPattern.realNumberWithQuote
    private val text                =   AtomicPattern.textWithQuote

    // Patterns with Quote sign - [Scala style]
    private val loadRegexPattern: Regex         = AtomicPattern.loadRegexPattern
    private val loadStatusRegexPattern: Regex   = AtomicPattern.loadStatusRegexPattern



    /**
      * Parse line to Object - [SCALA-STYLE]
      * #1 : Old Upload-Download - with status is null
      * #1 : New Upload-Download
      * #2 : Connect log
      * #3 : Error Log - pass through
      * @param line
      * @return
      */
    def extractValues(line: String): Option[AbtractLogLine]={
        line match {
            case loadRegexPattern(time1WithQuote,NASNameWithQuote,NASPortWithQuote,conNameWithQuote,sessIDWithQuote,
            input,output,termcode,sessionTime,ipWithQuote,macWithQuote,text)
                => return Option(LoadLogLineObject("",time1WithQuote,NASNameWithQuote,NASPortWithQuote,conNameWithQuote,sessIDWithQuote,
                input,output,termcode,sessionTime,ipWithQuote,macWithQuote,text))

            case loadStatusRegexPattern(statusWithQuote,time1WithQuote,NASNameWithQuote,NASPortWithQuote,conNameWithQuote,sessIDWithQuote,
            input,output,termcode,sessionTime,ipWithQuote,macWithQuote,text)
                => return Option(LoadLogLineObject(statusWithQuote,time1WithQuote,NASNameWithQuote,NASPortWithQuote,conNameWithQuote,sessIDWithQuote,
                input,output,termcode,sessionTime,ipWithQuote,macWithQuote,text))

            case _ => Some(ErroLogLine(line))
        }
    }
    // BELOW ARE JAVA STYLE :




    // Pattern with Quote sign - [JAVA - STYLE] - Cui vl
    // Just for fun!!!
    private val javaLoadRegexPattern            = Pattern.compile(AtomicPattern.loadRegexPattern.toString())
    private val javaLoadStatusRegexPattern      = Pattern.compile(AtomicPattern.loadStatusRegexPattern.toString())
    private val javaConRegexPattern             = Pattern.compile(AtomicPattern.conRegexPattern.toString())

    /**
      * Build Usage Log - Using java.util.regex.Matcher - [JAVA STYLE]
      * Change access modify to defaul in case necessary
      * @param matcher
      * @return
      */
    private def buildUsageLineRecord(matcher: Matcher)={
        LoadLogLineObject(
            matcher.group(1),
            matcher.group(2),
            matcher.group(3),
            matcher.group(4),
            matcher.group(5),
            matcher.group(6),
            matcher.group(7),
            matcher.group(8),
            matcher.group(9),
            matcher.group(10),
            matcher.group(11),
            matcher.group(12),
            matcher.group(13)
        )
    }

    /**
      * Parse and build record, return None on Failure [JAVA-STYLE]
      * Change access modify to defaul in case necessary
      * @param record
      * @return
      */
    private def parseRecord(record: String) : Option[AbtractLogLine]={
        val loadMatcher = javaLoadRegexPattern.matcher(record)
        if(loadMatcher.find){
            Some(buildUsageLineRecord(loadMatcher))
        }else {
            val loadStatusMatcher = javaLoadStatusRegexPattern.matcher(record)
            if(loadStatusMatcher.find) {
                Some(buildUsageLineRecord(loadStatusMatcher))
            }else {
                val connectMatcher = javaConRegexPattern.matcher(record)
                if(connectMatcher.find()) {
                    Some(buildUsageLineRecord(connectMatcher))
                }else{
                    None
                }
            }
        }


    }

}



/*
class LogParser {

    // specify regular expression patterns for log-line attributes
    val ip_address = "([0-9\\.]+)"
    val identifier, user_id, uri, status, size = "(\\S+)"
    val created_at = "(?:\\[)(.*)(?:\\])"
    val method = "(?:\\p{Punct})([A-Z]+)"
    val protocol = "(\\S+)(?:\\p{Punct})"
    val referer = "(?:\\p{Punct})(\\S+)(?:\\p{Punct})"
    val agent = "(?:\\p{Punct})([^\"]*)(?:\")"
    val user_meta_info = "(.*)"

    // string interpolate and create scala.util.matching.Regex (with trailing .r)
    val lineMatch = s"$ip_address\\s+$identifier\\s+$user_id\\s+$created_at\\s+$method\\s+$uri\\s+$protocol\\s+$status\\s+$size\\s+$referer\\s+$agent\\s+$user_meta_info".r

    // define function to match raw log lines and output LogLine class
    def extractValues(line: String): Option[LogLine] = {
        line match {
            case lineMatch(ip_address, identifier, user_id, created_at, method, uri, protocol, status, size, referer, agent, user_meta_info, _*)
            => return Option(LogLine(ip_address, identifier, user_id, created_at, method, uri, protocol, status, size, referer, agent, user_meta_info))
            case _
            =>  None
        }
    }

}*/
