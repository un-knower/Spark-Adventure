package com.templ

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

//case class Record(key: Int, value: String)
object HiveOperation {

  def main(args: Array[String]) {

    //内置的hive
    val warehouseLocation = new File("hdfs://hadoop102:9000/spark_warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //import spark.implicits._

    spark.sql("CREATE TABLE IF NOT EXISTS aaaaa (key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    val df = spark.sql("SELECT * FROM dept")
    df.show()

    df.write.format("json").save("hdfs://hadoop102:9000/ssss.json")

    spark.sql("drop table if exists users")
    spark.sql("show tables").show()
    spark.sql("create table if not exists users(id int,name string) row format delimited fields terminated by ' ' stored as textfile")
    spark.sql("show tables").show()
    spark.sql("select * from users").show()
    spark.sql("load data local inpath 'src/main/resources/a.txt' overwrite into table users")
    spark.sql("select * from users").show()


    spark.stop()
  }
}



