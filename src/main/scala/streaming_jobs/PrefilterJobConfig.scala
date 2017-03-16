package streaming_jobs

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 16/03/2017.
  */
case class PrefilterJobConfig(
                             inputTopic: String,
                             outputConTopic: String,
                             outputLoadTopic: String,
                             outputErrorTopic: String,
                             windowDuration: FiniteDuration,
                             slideDuration: FiniteDuration,
                             streamingBatchDuration: FiniteDuration,
                             streamingCheckPointDir: String,
                             sparkConfig: Map[String,String],
                             sourceKafka: Map[String,String],
                             prefilterSinkKafka: Map[String,String]
                             )extends Serializable{

}
object PrefilterJobConfig {
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

  def apply(): PrefilterJobConfig = apply(ConfigFactory.load)

  def apply(prefilterConfig: Config): PrefilterJobConfig = {
    val config = prefilterConfig.getConfig("prefilterConfig")
    new PrefilterJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topicCon"),
      config.as[String]("output.topicLoad"),
      config.as[String]("output.topicError"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka"),
      config.as[Map[String,String]]("prefilterSinkKafka")
    )
  }
}
