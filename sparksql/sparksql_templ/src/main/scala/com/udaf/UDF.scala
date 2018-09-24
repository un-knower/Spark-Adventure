package com.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF extends App {
  val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  val people: DataFrame = spark.read.json("sparksql\\sparksql_templ\\src\\main\\resources\\people.json")
  people.createOrReplaceTempView("people")

  // =======================================================================
  // UDF  one2one
  // =======================================================================
  spark.udf.register("add", (x: String) => "Pre:" + x)
  spark.sql("select add(name) from people").show

}
