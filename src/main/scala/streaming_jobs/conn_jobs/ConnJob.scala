package streaming_jobs.conn_jobs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import parser.ConnLogParser
import sources.KafkaDStreamSource
import streaming.SparkStreamingApplication

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 20/03/2017.
  */
class ConnJob(config: ConnJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def start(): Unit= {
    withSparkStreamingContext{(ss,ssc)=>
      val input : DStream[String] = source.createSource(ssc,config.inputTopic)

      //cache to speed-up processing if action fails
      input.persist(StorageLevel.MEMORY_ONLY_SER)
      val conlogParser = new ConnLogParser
      ParseAndCountConnLog.parseAndCount(
        ssc,
        ss,
        input,
        config.windowDuration,
        config.slideDuration,
        config.cassandraStorage,
        config.mongoStorage,
        conlogParser
      )
    }
  }
}

object ConnJob{
  def main(args: Array[String]): Unit = {
    val config = ConnJobConfig()
    val conJob = new ConnJob(config,KafkaDStreamSource(config.sourceKafka))
    conJob.start()
  }
}