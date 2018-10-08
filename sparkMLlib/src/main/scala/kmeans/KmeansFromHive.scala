package kmeans

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Administrator on 2016-5-29.
  * create table tbl_stock(
  * orderid string,
  * orderlocation string,
  * dateid string
  * )
  * row format delimited
  * fields terminated by ","
  * lines terminated by "\n";
  *
  *
  * create table tbl_stockdetail(
  * orderid string,
  * itmenum string,
  * itemid string,
  * itemqty int,
  * itemprice int,
  * itemamout int
  * )
  * row format delimited
  * fields terminated by ","
  * lines terminated by "\n";
  *
  */
object KmeansFromHive {

  def main(args: Array[String]) {

    //  如果在windows本地跑，需要从windows访问HDFS，需要指定一个合法的身份
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder().
      enableHiveSupport().
      config(new SparkConf().setAppName("hiveTest").setMaster("local[*]")).getOrCreate()
    //spark://hadoop102:7077 会报错 因为 这个类没有序列化 在集群中传递时就会出错

    import spark.implicits._
    //  hive.sql("select * from dept").show
    spark.sql("use spark")
    val data: DataFrame = spark.sql(
      s"""
         |SELECT
         |    a.locationId, SUM(b.number) totalnumber, SUM(b.amount) totalamount
         |FROM
         |    tbStock a
         |    JOIN tbStockDetail b
         |        ON a.orderNumber = b.orderNumber
         |GROUP BY a.locationId
     """.stripMargin)
    /*data.collect().foreach(x => {
      println(x)
    })*/

    //将hive中查询过来的数据，每一条变成一个向量，整个数据集变成矩阵
    val parseData: RDD[linalg.Vector] = data.rdd.map {
      case Row(_, totalqty, totalamount) =>
        val features: Array[Double] = Array[Double](totalqty.toString.toDouble, totalamount.toString.toDouble)
        //  将数组变成机器学习中的向量
        Vectors.dense(features)
    }

    //用kmeans对样本向量进行训练得到模型
    val numcluster = 3
    val maxIterations = 20 //指定最大迭代次数
    val model = KMeans.train(parseData, numcluster, maxIterations)

    //用模型对我们到数据进行预测
    val resrdd = data.rdd.map {

      case Row(orderlocation, totalqty, totalamount) =>
        //提取到每一行到特征值
        val features = Array[Double](totalqty.toString.toDouble, totalamount.toString.toDouble)
        //将特征值转换成特征向量
        val linevector = Vectors.dense(features)
        //将向量输入model中进行预测，得到预测值
        val prediction = model.predict(linevector)

        //返回每一行结果String
        orderlocation + " " + totalqty + " " + totalamount + " " + prediction//这个是哪一类
    }

    resrdd.collect().foreach(x => {
      println(x)
    })
    // resrdd.saveAsTextFile("/mllib/kmeans/")

    spark.stop()

  }

}
