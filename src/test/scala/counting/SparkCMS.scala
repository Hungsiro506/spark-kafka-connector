package counting

import org.apache.spark.sql.SparkSession


/**
  * Created by hungdv on 21/04/2017.
  */
object SparkCMS {
  private val master = "local[2]"
  private val appName = "CMSCounting"
  // CMS parameters
  val DELTA = 1E-3
  val EPS = 0.01
  val SEED = 1
  val PERC = 0.001
  // K highest frequency elements to take
  val TOPK = 10

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    val df = sparkSession.createDataFrame(Seq((1, 3, 4), (1, 2, 3), (2, 3, 4), (2, 3, 5))).toDF("col1", "col2", "col3")
    df.show()
    val exprs = df.columns.map((_ -> "approx_count_distinct")).toMap

    df.agg(exprs).show()

    val nameSketch = df.stat.countMinSketch(colName = "col2",eps = 0.001,confidence = 0.99,seed = 42)
    println(nameSketch.estimateCount(3))
    val nameBoomlomFilter = df.stat.bloomFilter(colName = "col2",expectedNumItems = 100L, fpp = 0.01)
    println(nameBoomlomFilter.mightContain(2))
    println(nameBoomlomFilter.mightContain(4))

  }
}
