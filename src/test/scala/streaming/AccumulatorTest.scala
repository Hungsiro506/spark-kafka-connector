package streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{HashMap => MutableHashMap}
import org.scalatest._
import streaming_jobs.conn_jobs.{ConcurrentMapAccumulator, SynchronizedMapAccumulator,ConcurrentHashMapAccumulator}
import streaming_jobs.conn_jobs.MapAccumulator

/**
  * Created by hungdv on 23/03/2017.
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test-Accumulator").master("local[3]").getOrCreate()
    val sc = spark.sparkContext
    val myAcc = new ConcurrentHashMapAccumulator()
                //new MapAccumulator()
    myAcc.getInstance(sc,"wordCount")
    sc.register(myAcc,"wordCount")
    val rdd = sc.parallelize(Seq("a" -> 10, "b"->40,"a" -> 40),2)
    rdd.foreachPartition{
      partition => partition.foreach{
        tuple => myAcc.add(tuple)
      }
    }
    println("Myacc : ")
    myAcc.value.toString()
    println(myAcc.value)

    myAcc add("a" -> 1)
    myAcc add("a" -> 9)

    println(myAcc.value)


  }
}
