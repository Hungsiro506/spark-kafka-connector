package streaming_jobs.conn_jobs
import scala.concurrent.duration.FiniteDuration
/**
  * Created by hungdv on 20/03/2017.
  */
/**
  * Case class define config for Connlog parse and aggragate job.
  * -- Dupplicated code, should define a trait, then extends it.
  * @param inputTopic
  * @param windowDuration
  * @param slideDuration
  * @param streamingBatchDuration
  * @param streamingCheckPointDir
  * @param sparkConfig
  * @param sourceKafka
  */
  case class ConnJobConfig(
                                 inputTopic: String,
                                 windowDuration: FiniteDuration,
                                 slideDuration: FiniteDuration,
                                 streamingBatchDuration: FiniteDuration,
                                 streamingCheckPointDir: String,
                                 sparkConfig: Map[String,String],
                                 sourceKafka: Map[String,String],
                                 cassandraStorage: Map[String,String],
                                 mongoStorage: Map[String,String]
                               )extends Serializable{
  }
  object ConnJobConfig {
    import com.typesafe.config.{Config,ConfigFactory}
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

    def apply(): ConnJobConfig = apply(ConfigFactory.load)

    def apply(connConfig: Config): ConnJobConfig = {
      val config = connConfig.getConfig("connConfig")
      new ConnJobConfig(
        config.as[String]("input.topic"),
        config.as[FiniteDuration]("windowDuration"),
        config.as[FiniteDuration]("slideDuration"),
        config.as[FiniteDuration]("streamingBatchDuration"),
        config.as[String]("streamingCheckPointDir"),
        config.as[Map[String,String]]("sparkConfig"),
        config.as[Map[String,String]]("sourceKafka"),
        config.as[Map[String,String]]("cassandraStorage"),
        config.as[Map[String,String]]("mongoStorage")
      )
    }
  }

