package parser

import util.AtomicPattern

import scala.util.matching.Regex

/**
  * Created by hungdv on 08/03/2017.
  */
class ConnLogParser extends AbtractLogParser {
  private val c_time2               =   AtomicPattern.time2
  private val c_ssThreadId          =   AtomicPattern.ssThreadId
  private val c_contask             =   AtomicPattern.conTask
  private val c_content             =   AtomicPattern.content
  private val c_nASName             =   AtomicPattern.nASName
  private val c_undefinedText       =   AtomicPattern.unindentified
  private val c_rejectCause         =   AtomicPattern.rejectCause
  private val c_rejectResultDetail  =   AtomicPattern.rejectResultDetail

  private val conRegexPattern: Regex          = AtomicPattern.conRegexPattern
  private val signInLogOffPattern: Regex      = AtomicPattern.signInLogOffRegexPattern
  private val rejectPattern: Regex            = AtomicPattern.rejectRegexPattern

  def extractValues(line: String): Option[AbtractLogLine]={
    line match {
      case signInLogOffPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_nASName,c_undefinedText)
        => return Option(ConnLogLineObject(c_time2,c_ssThreadId,c_contask,c_conName,c_nASName,c_undefinedText))

      case rejectPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_rejectCause,c_rejectResultDetail)
        => return Option(ConnLogLineObject(c_time2,c_ssThreadId,c_contask,c_conName,c_rejectCause,c_rejectResultDetail))

      case _ => Some(ErroLogLine(line))
    }
  }
}
