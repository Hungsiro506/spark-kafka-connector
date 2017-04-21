package counting

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SparkSession

/**
  * Created by hungdv on 20/04/2017.
  */
object ApproximateCountDistinSparkSQL {
  private val master = "local[2]"
  private val appName = "ApproximateCounting"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    val df = sparkSession.createDataFrame(Seq((1,3,4),(1,2,3),(2,3,4),(2,3,5))).toDF("col1","col2","col3")
    df.show()
    val exprs = df.columns.map((_ -> "approx_count_distinct")).toMap

    df.agg(exprs).show()

  }
}
