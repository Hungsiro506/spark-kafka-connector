package counting
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SparkSession

/**
  * Created by hungdv on 20/04/2017.
  */
object SQLTranformerWithJoin {
  private val master = "local[2]"
  private val appName = "SQLTransform"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()

    val df1 = sparkSession.createDataFrame(Seq((1,"foo"), (2,"bar"),(3,"baz"))).toDF("id","text")
    df1.createOrReplaceTempView("table1")
    val df2 = sparkSession.createDataFrame(Seq((3,4),(2,2),(3,5))).toDF("id","num")

    val st = new SQLTransformer()

    st.setStatement("SELECT t1.id, t1.text, th.num FROM __THIS__ th JOIN table1 t1 ON th.id  == t1.id ")


    st.transform(df2).show()
  }
}
