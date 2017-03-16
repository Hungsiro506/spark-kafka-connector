package streaming_jobs

import com.hungsiro.spark_kafka.core.streaming.KafkaPayloadStringCodec
import kafka.admin.TopicCommand
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import sources.KafkaDStreamSource
import streaming.SparkStreamingApplication
import util.AtomicPattern

import scala.util.matching.Regex
import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 16/03/2017.
  */
class PrefilterJob(config: PrefilterJobConfig,source: KafkaDStreamSource) extends SparkStreamingApplication {
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def generalLogRegex: Map[String,Regex]  = Map(RegexLogEnum.ConRegexLog.toString -> AtomicPattern.connGeneralRegex,
                                                RegexLogEnum.LoadRegexLog.toString -> AtomicPattern.loadGeneralRegex)

  def componentMatcher: Map[String,String] = Map(ComponentMatcherEnum.ConTail.toString -> AtomicPattern.connTail,
                                                  ComponentMatcherEnum.ConHead.toString -> AtomicPattern.connHead,
                                                  ComponentMatcherEnum.LoadHead.toString -> AtomicPattern.loadHead,
                                                  ComponentMatcherEnum.LoadTail.toString -> AtomicPattern.loadTail)
  def topics: Map[String,String]  = Map(TopicFilteredEnum.TopicErrorLog.toString -> config.outputErrorTopic,
                                        TopicFilteredEnum.TopicLoadLog.toString -> config.outputLoadTopic,
                                        TopicFilteredEnum.TopicConLog.toString  -> config.outputConTopic)

  def start(): Unit ={
    withSparkStreamingContext{(sc,ssc) =>
      val input: DStream[String] = source.createSource(ssc,config.inputTopic)

      //cache to speed-up processing if action fails
      input.persist(StorageLevel.MEMORY_ONLY_SER)

      Prefilter.classify(
        ssc,
        input,
        config.windowDuration,
        config.slideDuration,
        generalLogRegex,
        componentMatcher,
        topics,
        config.prefilterSinkKafka
      )

    }
  }

}
object PrefilterJob{
  def main(args: Array[String]): Unit = {

    val config = PrefilterJobConfig()

    val prefilterJob = new PrefilterJob(config,KafkaDStreamSource(config.sourceKafka))

    prefilterJob.start()
  }
}
