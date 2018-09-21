package com.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Employee(name: String, salary: Long)
case class Aver(var sum: Long, var count: Int)
// =======================================================================
// 强类型UDAF usually with group by (按 属性的相同字段 group)  【DSL使用】有点鸡肋
// =======================================================================
class AverageAggregator extends Aggregator[Employee, Aver, Double] {

  // 初始化方法 初始化每一个分区中的 共享变量
  override def zero: Aver = Aver(0L, 0)

  // 每一个分区中的每一条数据聚合的时候需要调用该方法
  override def reduce(b: Aver, a: Employee): Aver = {
    b.sum = b.sum + a.salary
    b.count = b.count + 1
    b
  }

  // 将每一个分区的输出 合并 形成最后的数据
  override def merge(b1: Aver, b2: Aver): Aver = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 给出计算结果
  override def finish(reduction: Aver): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 主要用于对共享变量进行编码
  override def bufferEncoder: Encoder[Aver] = Encoders.product

  // 主要用于将输出进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}

object AverageAggregator{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setAppName("udaf").setMaster("local[*]")).getOrCreate()

    import spark.implicits._

    //需要是DataSet
    val employee = spark.read.json("sparksql\\sparksql_templ\\src\\main\\resources\\employees.json")
      .as[Employee]

    //注册强类型UDAF的方法
    val aver = new AverageAggregator().toColumn.name("average")

    employee.select(aver).show()  //只能用DSL模式

//    spark.sql("select average(salary) from employee").show()//会报错 不能用SQL模式

    spark.stop()
  }

}
