package templ

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SqlWordCount extends App {
  val sparkConf = new SparkConf().setAppName("spark sql").setMaster("local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val peopleRdd: Dataset[String] = spark.read.textFile("sparksql\\sparksql_templ\\src\\main\\resources\\people.txt")

  val data: Dataset[String] = peopleRdd.flatMap(_.split(","))
  data.show

  data.createOrReplaceTempView("data")

  spark.sql("SELECT value word,COUNT(*) count FROM data GROUP BY value ORDER BY count DESC").show()

}
/*
+-------+
|  value|
+-------+
|Michael|
|     29|
|   Andy|
|     30|
| Justin|
|     19|
+-------+

+-------+-----+
|   word|count|
+-------+-----+
|     19|    1|
|Michael|    1|
|     29|    1|
|   Andy|    1|
|     30|    1|
| Justin|    1|
+-------+-----+
*/