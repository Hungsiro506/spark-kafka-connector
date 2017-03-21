package streaming_jobs

import com.hungsiro.spark_kafka.core.KafkaProducerFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import sinks.KafkaDStreamSinkExceptionHandler

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex
import org.elasticsearch.spark._
/**
  * Created by hungdv on 16/03/2017.
  */
object Prefilter {
  def classify(
                ssc: StreamingContext,
                lines: DStream[String],
                windowDuration: FiniteDuration,
                slideDuration: FiniteDuration,
                generalLogRegex: Map[String,Regex],
                componentMatcher: Map[String,String],
                topics: Map[String,String],
                producerConfig: Map[String,Object]
              ): Unit ={

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val sc = ssc.sparkContext

    // Time consuming :))) should i code some fucking hard code here??
    // Broadcast all static variables
    // Broadcast WindowDuration and Slide Duration in case you need to reduce DStream by Key and Window.
    //
    val bWindowDuration   = sc.broadcast(windowDuration)
    val bSlideDuration    = sc.broadcast(slideDuration)
    //
    val bGeneralLogRegex  = sc.broadcast(generalLogRegex)
    val bComponentMatcher = sc.broadcast(componentMatcher)
    val bTopics           = sc.broadcast(topics)
    val bProducerConfig   = sc.broadcast(producerConfig)

    val regexs: Map[String, Regex] = bGeneralLogRegex.value
    val matchers: Map[String, String] = bComponentMatcher.value
    val topicsOutput: Map[String,String] = bTopics.value

    val loadRegexPrefilter         = regexs(RegexLogEnum.LoadRegexLog.toString)
    val conRegexPrefilter          = regexs(RegexLogEnum.ConRegexLog.toString)

    // Hardcode is much better :)))))
    val loadHead: String           = matchers(ComponentMatcherEnum.LoadHead.toString)
    val loadTail: String           = matchers(ComponentMatcherEnum.LoadTail.toString)
    val conHead: String            = matchers(ComponentMatcherEnum.ConHead.toString)
    val conTail: String            = matchers(ComponentMatcherEnum.ConTail.toString)

    val topicConLog                = topics(TopicFilteredEnum.TopicConLog.toString)
    val topicLoadLog               = topics(TopicFilteredEnum.TopicLoadLog.toString)
    val topicError                 = topics(TopicFilteredEnum.TopicErrorLog.toString)

    lines.foreachRDD { rdd =>
      rdd.foreachPartition{ partition =>
        val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
        // Should create Array List here to avoid shuffle data
        /*val msgConLog         = scala.collection.mutable.ArrayBuffer.empty[String]
        val msgLoadLog        = scala.collection.mutable.ArrayBuffer.empty[String]
        val msgErrorLog      = scala.collection.mutable.ArrayBuffer.empty[String]

        val msgConLogRDD = sc.parallelize(msgConLog)
        val msgLoadLogRDD = sc.parallelize(msgLoadLog)
        val msgErrorLogRDD  = sc.parallelize(msgErrorLog)
*/
        val context = TaskContext.get
        val callback = new KafkaDStreamSinkExceptionHandler
        val logger = Logger.getLogger(getClass)
        logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic in [load,con,error]")

        val metadata = partition.map{ logLine =>
          logLine match{
            case loadRegexPrefilter(loadHead,loadTail) => new ProducerRecord[String,String](topicLoadLog,"load",logLine)
            case conRegexPrefilter(conHead,conTail)    => new ProducerRecord[String,String](topicConLog,"connect",logLine)
            case _                                     => new ProducerRecord[String,String](topicError,"error",logLine)
          }
        }.map{ record =>
          callback.throwExceptionIfAny()
          producer.send(record,callback)
        }.toList
      }

      /*rdd.saveToEs("test/radius")*/
    }
  }
}
object TopicFilteredEnum extends Enumeration{

  type RadiusOutputTopic = Value

  val TopicConLog    = Value("topicCon")
  val TopicLoadLog   = Value("topicLoad")
  val TopicErrorLog  = Value("topicError")

  val allTopics      = Seq(TopicConLog,TopicLoadLog,TopicErrorLog)
}
object RegexLogEnum extends Enumeration{

  type RadiusRegex = Value

  val LoadRegexLog = Value("loadRegexPre")
  val ConRegexLog  = Value("conRegexPre")
}
object ComponentMatcherEnum extends Enumeration{

  type RadiusMatchers = Value
  val LoadHead = Value("loadHead")
  val LoadTail = Value("loadTail")
  val ConHead  = Value("conHead")
  val ConTail  = Value("conTail")
}