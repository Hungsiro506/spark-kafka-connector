package storage.redis
import org.apache.spark.{SparkConf, _}
import org.apache.spark.streaming._
import com.redislabs.provider.redis._
/**
  * Created by hungdv on 17/03/2017.
  */
/*
class redisStreamingTest(conf: SparkConf) {
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))

}
*/

// THIS FACE AN ERROR ABOUT  SPARK CONTEXT CAN

object StreamToRedisTest{



  val checkpointDirectory = "/tmp"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set("redis.host", "localhost")
      .set("redis.port", "6379")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val checkpointDirectory = "/tmp"
    ssc.checkpoint(checkpointDirectory)

    val lines = ssc.socketTextStream("localhost", 9999)
    val test = lines
      .filter(l => {
        l.startsWith("Example")
      })

    test.foreachRDD(rdd => {
      sc.toRedisLIST(rdd, "Test")
    })

    test.print()

    ssc.start()
    ssc.awaitTermination()
  }
}