package templ

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlTemplate extends App {
  val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  val people: DataFrame = spark.read.json("sparksql\\sparksql_templ\\src\\main\\resources\\people.json")
  people.select("name").show()
  people.show()

  people.createOrReplaceTempView("people")
  spark.sql("select * from people where age > 19").show()

  import spark.implicits._
  println(people.map(_.getAs[String]("name")).collect().mkString(" "))
}
