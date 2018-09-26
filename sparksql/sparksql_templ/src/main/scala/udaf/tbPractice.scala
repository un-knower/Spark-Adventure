package udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object tbPractice extends App {
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

  //  val spark = SparkSession.builder()
  //    .config(new SparkConf().setAppName("test").setMaster("local[*]")).getOrCreate()

  val spark = SparkSession.builder().
    enableHiveSupport().
    config(new SparkConf().setAppName("hiveTest").setMaster("spark://hadoop102:7077")).getOrCreate()

  //  hive.sql("select * from dept").show
  spark.sql("use spark")

  //需求一： 统计所有订单中每年的销售单数、销售总额
  //错了不能count * 要去重单号
  /**val result1my = spark.sql(
    s"""
       |SELECT
       |    c.theYear, COUNT(*), SUM(b.amount)
       |FROM
       |    tbStock a
       |    JOIN tbStockDetail b
       |        ON a.orderNumber = b.orderNumber
       |    JOIN tbDate c
       |        ON a.dateId = c.dateId
       |GROUP BY c.theYear
       |ORDER BY c.theYear
     """.stripMargin)
//  insertMySQL("xq1My", result1my)

  val result1 = spark.sql(
    s"""
       |SELECT
       |    c.theYear, COUNT(DISTINCT a.orderNumber), SUM(b.amount)
       |FROM
       |    tbStock a
       |    JOIN tbStockDetail b
       |        ON a.orderNumber = b.orderNumber
       |    JOIN tbDate c
       |        ON a.dateId = c.dateId
       |GROUP BY c.theYear
       |ORDER BY c.theYear
   """.stripMargin)
  result1.show*/
  //  insertMySQL("xq1",result1)

  //需求二： 统计每年最大金额订单的销售额
  /**val result2my = spark.sql(
    s"""
       |SELECT
       |    c.theYear, MAX(b.sumAmount)
       |FROM
       |    tbStock a
       |    JOIN
       |        (SELECT
       |            orderNumber, SUM(amount) sumAmount
       |        FROM
       |            tbStockDetail
       |        GROUP BY orderNumber) b
       |        ON a.orderNumber = b.orderNumber
       |    JOIN tbDate c
       |        ON a.dateId = c.dateId
       |GROUP BY c.theYear
       |ORDER BY c.theYear
       """.stripMargin)
  result2my.show
//  insertMySQL("xq2My", result2my)

  val result2 = spark.sql(
    s"""
       |SELECT
       |    theYear, MAX(c.SumOfAmount) AS SumOfAmount
       |FROM
       |    (SELECT
       |        a.dateId, a.orderNumber, SUM(b.amount) AS SumOfAmount
       |    FROM
       |        tbStock a
       |        JOIN tbStockDetail b
       |            ON a.orderNumber = b.orderNumber
       |    GROUP BY a.dateId, a.orderNumber) c
       |    JOIN tbDate d
       |        ON c.dateId = d.dateId
       |GROUP BY theYear
       |ORDER BY theYear DESC
     """.stripMargin
  )*/
//  insertMySQL("xq2", result2)

  //需求三： 统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
  //优化后 第一步
  /**spark.sql(
    s"""
       |    SELECT
       |        c.theYear,
       |        b.itemId,
       |        SUM(b.amount) AS sumOfAmount
       |    FROM
       |        tbStock a
       |    JOIN tbStockDetail b ON a.orderNumber = b.orderNumber
       |    JOIN tbDate c ON a.dateId = c.dateId
       |    GROUP BY
       |        c.theYear,
       |        b.itemId
     """.stripMargin).show*/

    //第二步对
  /*val result3MyBetter=spark.sql(
    s"""
       |SELECT
       |    *
       |FROM
       |    (SELECT
       |        *, rank () over (PARTITION BY t2.theYear
       |                         ORDER BY sumAByYI DESC) rank
       |    FROM
       |        (SELECT
       |            c.theYear, b.itemId, SUM(b.amount) AS sumAByYI
       |        FROM
       |            tbStock a
       |            JOIN tbStockDetail b
       |                ON a.orderNumber = b.orderNumber
       |            JOIN tbDate c
       |                ON a.dateId = c.dateId
       |        GROUP BY c.theYear, b.itemId) t2) t3
       |WHERE t3.rank = 1
     """.stripMargin)
  insertMySQL("xq3optimize", result3MyBetter)*/

    val result3MyBetter2=spark.sql(
      s"""
         |SELECT
         |        *, MAX() over (PARTITION BY theYear
         |                         ORDER BY sumAByYI DESC) maxSum
         |    FROM
         |        (SELECT
         |            c.theYear, b.itemId, SUM(b.amount) AS sumAByYI
         |        FROM
         |            tbStock a
         |            JOIN tbStockDetail b
         |                ON a.orderNumber = b.orderNumber
         |            JOIN tbDate c
         |                ON a.dateId = c.dateId
         |        GROUP BY c.theYear, b.itemId) t2
     """.stripMargin)
  insertMySQL("xq3optimizeMAX", result3MyBetter2)

  //老师的
  /**val result3 = spark.sql(
    s"""
       |SELECT DISTINCT
       |    e.theYear,
       |    e.itemId,
       |    f.maxOfAmount
       |FROM
       |    (
       |        SELECT
       |            c.theYear,
       |            b.itemId,
       |            SUM(b.amount) AS sumOfAmount
       |        FROM
       |            tbStock a
       |        JOIN tbStockDetail b ON a.orderNumber = b.orderNumber
       |        JOIN tbDate c ON a.dateId = c.dateId
       |        GROUP BY
       |            c.theYear,
       |            b.itemId
       |    ) e
       |JOIN (
       |    SELECT
       |        d.theYear,
       |        MAX(d.sumOfAmount) AS maxOfAmount
       |    FROM
       |        (
       |            SELECT
       |                c.theYear,
       |                b.itemId,
       |                SUM(b.amount) AS sumOfAmount
       |            FROM
       |                tbStock a
       |            JOIN tbStockDetail b ON a.orderNumber = b.orderNumber
       |            JOIN tbDate c ON a.dateId = c.dateId
       |            GROUP BY
       |                c.theYear,
       |                b.itemId
       |        ) d
       |    GROUP BY
       |        d.theYear
       |) f ON e.theYear = f.theYear
       |AND e.sumOfAmount = f.maxOfAmount
       |ORDER BY
       |    e.theYear
     """.stripMargin)*/
  //  insertMySQL("xq3",result3)

  spark.stop
}
