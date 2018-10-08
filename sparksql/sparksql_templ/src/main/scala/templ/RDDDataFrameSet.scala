package templ

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDDDataFrameSet extends App {
  val sparkConf = new SparkConf().setAppName("spark sql").setMaster("local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
  val peopleRdd = sc.textFile("sparksql\\sparksql_templ\\src\\main\\resources\\people.txt")

  // =======================================================================
  // RDD 《-》 DataFrame  =  DataSet[Row]
  // =======================================================================
  println("====== 1、RDD  -》  DataFrame （1、手动确定确定Schema）=================================")
  println(peopleRdd.collect().mkString(" | "))
  val name2AgeRDD = peopleRdd.map { x =>
    val para = x.split(",")
    (para(0), para(1).trim.toInt)
  }
  println(name2AgeRDD.collect().mkString(" | "))

  import spark.implicits._

  val frame: DataFrame = name2AgeRDD.toDF("name", "age")
  frame.show

  println("====== 1、RDD  -》  DataFrame （2、通过反射确定  （利用case class 的功能））=================================")

  case class People(name: String, age: Int)

  val dataPeople: DataFrame = peopleRdd.map { x =>
    val para = x.split(",")
    People(para(0), para(1).trim.toInt)
  }.toDF("name", "age")
  dataPeople.show

  println("====== 1、RDD  -》  DataFrame （ 3、通过编程方式来确定）=================================")
  val schema = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
  val data = peopleRdd.map { x =>
    val para = x.split(",")
    Row(para(0), para(1).trim.toInt)
  }
  val dataSchema: DataFrame = spark.createDataFrame(data, schema)
  dataSchema.show

  println("====== 1、DataFrame  -》  RDD （ dataFrame.rdd）=================================")
  println(dataSchema.rdd.map(_.getString(0)).collect().mkString(" | ")) //DataFrame转换的rdd是弱类型
  println(dataSchema.rdd.collect().mkString(" | ")) //是 RDD[Row]类型
  println()


  // =======================================================================
  // RDD 《-》 DataSet
  // =======================================================================
  println("====== 2、RDD  -》  DataSet （case class 确定schema）=================================")
  val dataSet: Dataset[People] = peopleRdd.map { x =>
    val para = x.split(",")
    People(para(0), para(1).trim.toInt)
  }.toDS
  println(dataSet.getClass) //DataSet[People] Schema是case class People
  dataSet.show

  println("====== 2、DataSet  -》  RDD（case class 确定schema）=================================")
  val rddPeople: RDD[People] = dataSet.rdd
  println(rddPeople.collect().mkString(" | ")) //DataSet转换的rdd是强类型
  println(rddPeople.map(_.name).collect().mkString(" | ")) //DataSet转换的rdd是强类型
  println()


  // =======================================================================
  // DataFrame 《-》 DataSet
  // =======================================================================
  println("====== 3、DataSet -》  DataFrame =================================")
  val set2Frame: DataFrame = dataSet.toDF()
  set2Frame.show

  println("====== 3、DataFrame -》  DataSet =================================")
  val frame2Set: Dataset[People] = set2Frame.as[People]
  frame2Set.show

  spark.stop()
}
