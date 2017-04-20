package streaming_jobs.conn_jobs

import org.apache.spark.{AccumulableParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{HashMap => MutableHashMap}
import org.apache.spark._
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import java.util.Timer
import java.util.TimerTask
import org.joda.time.{DateTime,Duration}
/**
  * Created by hungdv on 21/03/2017.
  */
//OLD API @since spark version 1.x
// Approach 1: USING SPARK OLD ACCUMULABLEPARAM
/*class MapAccumulator extends AccumulableParam[MutableHashMap[String, Int], (String, Int)] {

  private var accumulator: Accumulable[MutableHashMap[String, Int], (String, Int)] = _

  def addAccumulator(acc: MutableHashMap[String, Int], elem: (String, Int)): MutableHashMap[String, Int] = {
    val (k1, v1) = elem
    acc += acc.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)

    acc
  }

  def addInPlace(acc1: MutableHashMap[String, Int], acc2: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
    acc2.foreach(elem => addAccumulator(acc1, elem))
    acc1
  }

   def zero(initialValue: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
    copy.clear()
    copy
  }

  def getInstance(sc: SparkContext): Accumulable[MutableHashMap[String, Int], (String, Int)] = {
    if (accumulator == null) {
      synchronized {
        if (accumulator == null) {
           accumulator = sc.accumulable(MutableHashMap.empty[String, Int],"WordCountAccumulator")(new MapAccumulator)
        }
      }
    }
    accumulator
  }

}*/
/*
  Other aproach for small number of elements :
   val httpInfo = sparkContext accumulator(0, "HTTP 1xx")
   val httpSuccess = sparkContext accumulator(0, "HTTP 2xx")
   val httpRedirect = sparkContext accumulator(0, "HTTP 3xx")
   val httpClientError = sparkContext accumulator(0, "HTTP 4xx")
   val httpServerError = sparkContext accumulator(0, "HTTP 5xx")
   each accumulator represents total count of a type.
 */


//APPROACH 2.0 : Mutable HashMap with Synchronized Map.
// Not good for extending and might not meet performance.
class MapAccumulator(private var accumulator: MutableHashMap[String,Int])
  extends AccumulatorV2[(String, Int),MutableHashMap[String, Int]]
  {
     def this(){
        this(new MutableHashMap[String,Int]() with mutable.SynchronizedMap[String, Int])
    }

    override def isZero: Boolean = {
      accumulator.isEmpty
    }

    override def reset(): Unit = {
      accumulator.clear()
    }

    override def copyAndReset(): AccumulatorV2[(String, Int), MutableHashMap[String, Int]] =
      new MapAccumulator()

    override def add(elem: (String, Int)): Unit = {
      val (k1, v1) = elem
      accumulator += accumulator.find(_._1 == k1).map {
        case (k2, v2) => k2 -> (v1 + v2)
      }.getOrElse(elem)
    }

    override def merge(other: AccumulatorV2[(String, Int), MutableHashMap[String, Int]]): Unit = {
      other match{
        case o: MapAccumulator => addOtherHashMap(other.value)
        case _ => throw new Exception(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
        )
      }
    }

    def addOtherHashMap(hm: MutableHashMap[String,Int]): Unit ={
      hm.foreach(elem => add(elem))
    }

    override def value: MutableHashMap[String, Int] = {
      accumulator
    }
    def zero(initialValue: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
      val ser = new JavaSerializer(new SparkConf(false)).newInstance()
      val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
      copy.clear()
      copy
    }
    def getInstance(sc: SparkContext, name: String): Unit ={
      if(this.isRegistered){
        synchronized{
          if(this.isRegistered){
           sc.register(this,name)
          }
        }
      }
    }

    override def copy(): MapAccumulator = {
      val newAcc = new MapAccumulator()
      accumulator.synchronized{
        this.accumulator.foreach{
          elem => newAcc.add(elem)
        }
      }
      newAcc
    }
  }

//APPROACH 2.1 USING java.util.concurent.ConcurrentHashMap
/**
  * Concurrent HashMapAccumulator using java concurrent.ConcurrentHashMap.
  * Auto reset at midnight
  * @param accumulator
  */
class ConcurrentHashMapAccumulator(private var accumulator:  mutable.Map[String,Int])
  extends AccumulatorV2[(String, Int), mutable.Map[String, Int]]
{
  private[this] val logger = LoggerFactory.getLogger(getClass().getName());
  def this(){
    this(new ConcurrentHashMap[String,Int]() asScala)
    val  runner  =  new  RunCheckReset()
    val  timer  =  new  Timer()
    timer.scheduleAtFixedRate(runner,0, 600000L) // not work
    timer.schedule(runner,0, 600000L) // not work
  }

  override def isZero: Boolean = {
    accumulator.isEmpty
  }

  override def reset(): Unit = {
    accumulator.clear()
  }

  override def copyAndReset(): AccumulatorV2[(String, Int),  mutable.Map[String, Int]] =
    new ConcurrentHashMapAccumulator()

  override def add(elem: (String, Int)): Unit = {
    val (k1, v1) = elem
    accumulator += accumulator.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)
  }

  override def merge(other: AccumulatorV2[(String, Int),  mutable.Map[String, Int]]): Unit = {
    other match{
      case o: ConcurrentHashMapAccumulator => addOtherHashMap(other.value)
      case _ => throw new Exception(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
      )
    }
  }

  def addOtherHashMap(hm:  mutable.Map[String,Int]): Unit ={
    hm.foreach(elem => add(elem))
  }

  override def value:  mutable.Map[String, Int] = {
    accumulator
  }
  def zero(initialValue: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
  def getInstance(sc: SparkContext, name: String): Unit ={
    if(this.isRegistered){
      synchronized{
        if(this.isRegistered){
          sc.register(this,name)
        }
      }
    }
  }

  override def copy(): ConcurrentHashMapAccumulator = {
    val newAcc = new ConcurrentHashMapAccumulator()
    accumulator.synchronized{
      this.accumulator.foreach{
        elem => newAcc.add(elem)
      }
    }
    newAcc
  }
  class RunCheckReset extends  TimerTask{
   /* private val c = Calendar.getInstance()
    private val midnigh = Calendar.getInstance()
    override def run(): Unit = {
      val now = c.getTimeInMillis()
      c.set(Calendar.HOUR_OF_DAY, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      c.set(Calendar.MILLISECOND, 0)
      val passed = now - c.getTimeInMillis()
      val secondsPassed = passed / 1000
      if(secondsPassed >= 0 && secondsPassed < 61) {
        reset()
        logger.info(s"Reset accumulator at ${now}.")
      }
    }*/
   override def run(): Unit = {
     val now = new DateTime()
     //val midnight = now.withTimeAtStartOfDay
     /*val midnight = new DateTime(2017,3 ,24 ,9 ,30 ,0 ,0); // testing purpose
     val duration = new Duration(midnight, now)
     val seconds = duration.toStandardSeconds().getSeconds()
     if (seconds >= 0 && seconds < 61) {
       reset()
       logger.info(s"Reset accumulator at ${now}.")
       println("Acc has been reset at " + now + "in " + TaskContext.getPartitionId() )
     }*/
     println("Now is " + now + " in " + TaskContext.getPartitionId())
     try{
       Thread.sleep(25000L) // not work!
     }catch{
       case ie: InterruptedException => println(ie)
     }
   }
  }

}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Approach 3.0
/*
  * Base MapAccumulator class - unsafe
  * @param accumulator
 class MapAccumulator( private var accumulator: mutable.Map[String,Int]  )
  extends AccumulatorV2[(String, Int),mutable.Map[String, Int]]
{
  /*private var accumulator: MutableHashMap[String,Int]  =
    new MutableHashMap[String,Int]() with mutable.SynchronizedMap[String, Int]*/

   def this() {
     this(new mutable.HashMap[String, Int])
   }

  override def isZero: Boolean = {
    accumulator.isEmpty
  }

  override def reset(): Unit = {
    accumulator.clear()
  }

  override def copyAndReset(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] =
    new MapAccumulator()

  override def add(elem: (String, Int)): Unit = {
    val (k1, v1) = elem
    accumulator += accumulator.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)
  }

  override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
    other match{
      case o: MapAccumulator => addOtherHashMap(other.value)
      case _ => throw new Exception(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
      )
    }
  }

  def addOtherHashMap(hm: mutable.Map[String,Int]): Unit ={
    hm.foreach(elem => add(elem))
  }

  override def value: mutable.Map[String, Int] = {
    accumulator
  }

  def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
    copy.clear()
    copy
  }

  def getInstance(sc: SparkContext, name: String): Unit ={
    if(this.isRegistered){
      synchronized{
        if(this.isRegistered){
          sc.register(this,name)
        }
      }
    }
  }

  override def copy(): MapAccumulator = {
    val newAcc = new MapAccumulator()
      this.accumulator.foreach{
        elem => newAcc.add(elem)
    }
    newAcc
  }
}

/**
  * Best way to implement a Concurrent Map Accumulator
  *
  * @param accMap
  */
class ConcurrentMapAccumulator(accMap: collection.concurrent.Map[String, Int]) extends MapAccumulator(accMap){
  def this() = this(new ConcurrentHashMap[String, Int] asScala)
}

/**
  * Might not meet performance.
  * @param accMap
  */
class SynchronizedMapAccumulator(accMap : mutable.Map[String,Int]) extends MapAccumulator(accMap){
  def this() = this(new mutable.HashMap[String, Int] with mutable.SynchronizedMap[String, Int])
}*/


//APPROACH 3.1
///////////////////////////////////////////////////////////////////////////////////////////////
// Using SynchronizedMap and java.concurence.HashMap()


/**
  * Best way to implement a Concurrent Map Accumulator
  * Runtime error. -> throw Exception java
  * @param accMap
  */
class ConcurrentMapAccumulator(accMap: collection.concurrent.Map[String, Int]) extends BaseMapAccumulator(accMap){
  def this() = this(new ConcurrentHashMap[String, Int] asScala)
}

/**
  * Might not meet performance.
  * @param accMap
  */
class SynchronizedMapAccumulator(accMap : mutable.Map[String,Int]) extends BaseMapAccumulator(accMap){
  def this() = this(new mutable.HashMap[String, Int] with mutable.SynchronizedMap[String, Int])
}

/*
  * Base MapAccumulator class - unsafe
  * @param accumulator
  * */
 class BaseMapAccumulator( private var accumulator: mutable.Map[String,Int]  )
  extends AccumulatorV2[(String, Int),mutable.HashMap[String, Int]]
{
  /*private var accumulator: MutableHashMap[String,Int]  =
    new MutableHashMap[String,Int]() with mutable.SynchronizedMap[String, Int]*/

   def this() {
     this(new mutable.HashMap[String, Int])
   }

  override def isZero: Boolean = {
    accumulator.isEmpty
  }

  override def reset(): Unit = {
    accumulator.clear()
  }

  override def copyAndReset(): AccumulatorV2[(String, Int), mutable.HashMap[String, Int]] =
    new BaseMapAccumulator()

  override def add(elem: (String, Int)): Unit = {
    val (k1, v1) = elem
    accumulator += accumulator.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)
  }

  override def merge(other: AccumulatorV2[(String, Int), mutable.HashMap[String, Int]]): Unit = {
    other match{
      case o: BaseMapAccumulator => addOtherHashMap(other.value)
      case _ => throw new Exception(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
      )
    }
  }

  def addOtherHashMap(hm: mutable.Map[String,Int]): Unit ={
    hm.foreach(elem => add(elem))
  }

  override def value: mutable.HashMap[String, Int] = {
    val res = new mutable.HashMap[String,Int]()

    accumulator.foreach{ tuple =>
      res += tuple
    }
    res
  }

  def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
    copy.clear()
    copy
  }

  def getInstance(sc: SparkContext, name: String): Unit ={
    if(this.isRegistered){
      synchronized{
        if(this.isRegistered){
          sc.register(this,name)
        }
      }
    }
  }

  override def copy(): BaseMapAccumulator = {
    val newAcc = new BaseMapAccumulator()
      this.accumulator.foreach{
        elem => newAcc.add(elem)
    }
    newAcc
  }
}

