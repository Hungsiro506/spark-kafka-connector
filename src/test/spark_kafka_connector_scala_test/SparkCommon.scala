package spark_kafka_connector_scala_test
import org.apache.spark.SparkConf
/**
  * Created by hungdv on 02/03/2017.
  */
object SparkCommon {
    val colBicycle  = "bike_aggregation"
    val mongoUrl    = "mongodb://localhost:27017/streamdb." + colBicycle
  lazy val  conf= new SparkConf()
  conf.set("spark.mongodb.input.uri", mongoUrl)
       .set("spark.mongodb.output.uri", mongoUrl)
}
