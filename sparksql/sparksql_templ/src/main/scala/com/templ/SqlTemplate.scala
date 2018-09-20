package com.templ

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlTemplate extends App {
  val path = "sparksql\\sparksql_templ\\src\\main\\resources\\people.json"
  val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  val people: DataFrame = spark.read.json(path)
  people.cache()
  people.select("name").show()
  people.show()
  people.createOrReplaceTempView("people")
  spark.sql("select * from people where age > 19").show()
  import spark.implicits._
  println(people.map(_.getAs[String]("name")).collect().mkString(" "))
}
