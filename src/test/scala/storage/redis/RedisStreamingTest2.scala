package storage.redis

import streaming.SparkStreamingApplication
import com.redislabs.provider.redis._
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 18/03/2017.
  */
class RedisStreamingTest2(config: RedisJobConfig) extends SparkStreamingApplication {
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def  start(): Unit = {
    withSparkStreamingContext {
      (sc, ssc) =>
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
        val test = lines.foreachRDD { rdd =>
          rdd.filter(l =>
            l.startsWith("Example")
          )
          //sc.toRedisLIST(rdd, "Test")
        }
        /*test.foreachRDD(rdd => {
          sc.toRedisLIST(rdd, "Test")
        })*/

        //test.print()


    }
  }
}
object RedisStreamingTest2{
  def main(args: Array[String]): Unit = {
    val config = RedisJobConfig()
    val streamingJob = new RedisStreamingTest2(config)
    streamingJob.start()
  }
}


case class RedisJobConfig(windowDuration: FiniteDuration,
                          slideDuration: FiniteDuration,
                          sparkConfig: Map[String, String],
                          streamingBatchDuration: FiniteDuration,
                          streamingCheckpointDir: String)
object RedisJobConfig{
  def apply(): RedisJobConfig={
    val windowDuration = new FiniteDuration(5,java.util.concurrent.TimeUnit.SECONDS)
    val slideDuration  = new FiniteDuration(5,java.util.concurrent.TimeUnit.SECONDS)
    val batchDuration  = new FiniteDuration(5,java.util.concurrent.TimeUnit.SECONDS)
    val sparkConfig   = Map[String,String]("spark.master"-> "local[2]",
                                           "spark.app.name"-> "redistest",
                                          "redis.host" -> "localhost",
                                          "redis.port" -> "6379")
    val sparkCheckpointDir = "/tmp"

    new RedisJobConfig(windowDuration,slideDuration,sparkConfig,batchDuration,sparkCheckpointDir)
  }
}