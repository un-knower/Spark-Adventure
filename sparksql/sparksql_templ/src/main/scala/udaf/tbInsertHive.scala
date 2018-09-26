package udaf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object tbInsertHive extends App {
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()
  }

  val spark = SparkSession.builder().
    enableHiveSupport().
    config(new SparkConf().setAppName("hiveTest").setMaster("local[*]")).getOrCreate() //local[*]
  import spark.implicits._

  /** CSV read */
  val stockRddCSV: DataFrame = spark.read.csv("sparksql\\sparksql_templ\\doc\\tbStock.txt")
  val data: DataFrame = spark.read.format("com.databricks.spark.csv")
    .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
    .option("inferSchema", true.toString) //这是自动推断属性列的数据类型。
    .load("sparksql\\sparksql_templ\\doc\\tbStock.txt")//文件的路径


  case class tbStock(orderNumber:String,locationId:String,dateId:String) extends Serializable
  val stockRdd: RDD[String] = spark.sparkContext.textFile("sparksql\\sparksql_templ\\doc\\tbStock.txt")
  val stock: Dataset[tbStock] = stockRdd.map { x =>
    val attr = x.split(",")
    tbStock(attr(0),attr(1),attr(2))
  }.toDS
//  stock.show
//  stock.createOrReplaceTempView("tbStock")
  insertHive(spark, "tbStock", stock.toDF)

  case class tbStockDetail(orderNumber:String, rowNum:Int, itemId:String, number:Int, price:Double, amount:Double) extends Serializable
  val stockDetailRdd: RDD[String] = spark.sparkContext.textFile("sparksql\\sparksql_templ\\doc\\tbStockDetail.txt")
  val stockDetail: Dataset[tbStockDetail] = stockDetailRdd.map { x =>
    val attr = x.split(",")
    tbStockDetail(attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)
  }.toDS
//  stockDetail.show
//  stockDetail.createOrReplaceTempView("tbStockDetail")
  insertHive(spark, "tbStockDetail", stockDetail.toDF)

  case class tbDate(dateId:String, years:Int, theYear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfMonth:Int) extends Serializable
  val dateRdd: RDD[String] = spark.sparkContext.textFile("sparksql\\sparksql_templ\\doc\\tbDate.txt")
  val date: Dataset[tbDate] = dateRdd.map { x =>
    val attr = x.split(",")
    tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)
  }.toDS
  //  date.show
//  date.createOrReplaceTempView("tbDate")
  insertHive(spark, "tbDate", date.toDF)

  //需求一： 统计所有订单中每年的销售单数、销售总额
  //错了不能count * 要去重单号
  /*val result1my = spark.sql("SELECT c.theYear, COUNT(*), SUM(b.amount)\nFROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber\nJOIN tbDate c ON a.dateId = c.dateId\nGROUP BY c.theYear\nORDER BY c.theYear")
  insertMySQL("xq1My",result1my)
  val result1 = spark.sql(
  "SELECT c.theYear, COUNT(DISTINCT a.orderNumber), SUM(b.amount) " +
  "FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber " +
    "JOIN tbDate c ON a.dateId = c.dateId " +
    "GROUP BY c.theYear ORDER BY c.theYear")
  insertMySQL("xq1",result1) */

  //需求二： 统计每年最大金额订单的销售额

  /*val result2my = spark.sql("SELECT c.theYear, MAX(b.sumAmount)" +
    "FROM tbStock a" +
    "JOIN (select orderNumber,SUM(amount) sumAmount from tbStockDetail group by orderNumber) b" +
    "ON a.orderNumber = b.orderNumber" +
    "JOIN tbDate c ON a.dateId = c.dateId" +
    "GROUP BY c.theYear" +
    "ORDER BY c.theYear")
  insertMySQL("xq2My",result2my)
  val result2 = spark.sql("SELECT theYear, MAX(c.SumOfAmount) AS SumOfAmount " +
    "FROM (SELECT a.dateId, a.orderNumber, SUM(b.amount) AS SumOfAmount " +
          "FROM tbStock a " +
          "JOIN tbStockDetail b " +
          "ON a.orderNumber = b.orderNumber " +
          "GROUP BY a.dateId, a.orderNumber ) c " +
    "JOIN tbDate d " +
    "ON c.dateId = d.dateId " +
    "GROUP BY theYear " +
    "ORDER BY theYear")
  insertMySQL("xq2",result2)*/

  //需求三： 统计每年最畅销货品
//  val result3 = spark.sql("SELECT DISTINCT e.theYear, e.itemId, f.maxOfAmount FROM (SELECT c.theYear, b.itemId, SUM(b.amount) AS sumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber JOIN tbDate c ON a.dateId = c.dateId GROUP BY c.theYear, b.itemId ) e JOIN (SELECT d.theYear, MAX(d.sumOfAmount) AS maxOfAmount FROM (SELECT c.theYear, b.itemId, SUM(b.amount) AS sumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.orderNumber = b.orderNumber JOIN tbDate c ON a.dateId = c.dateId GROUP BY c.theYear, b.itemId ) d GROUP BY d.theYear ) f ON e.theYear = f.theYear AND e.sumOfAmount = f.maxOfAmount ORDER BY e.theYear")
//  insertMySQL("xq3",result3)
  spark.stop
}
