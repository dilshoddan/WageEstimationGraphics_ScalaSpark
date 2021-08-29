import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{File, FileWriter}
import scala.collection.mutable

case class ARecord(id: Int, val wage: Double, education: Int, experience: Int, age: Int, ethnicity: String, region: String, gender: String, val occupation: String, sector: String, union: String, married: String)

case class WageRecord(occupation: String, wage: Double)

case class WageRecordR(occupation: String, mean: Double, variance: Double)

object WageStat {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("WageStat")
      .master("local[*]")
      .getOrCreate()

    val csv = spark.sparkContext.textFile("data/CPS1985.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val rawData = headerAndRows.filter(_ (0) != header(0))

    val data = rawData.map(r => (r(8), r(1).toDouble)).cache() //pairRDD
    val actual = calculateAvgVariance(data, 0).cache()

    val sample = data.sample(false, 0.25).cache()
    var h = new mutable.TreeMap[String, ((Double, Int), (Double, Int))]

    for (a <- 1 to 100) {
      val resample = sample.sample(true, 1)
      val estimation = calculateAvgVariance(resample, 1)
      h = AddToBag(h, estimation.collect().toList)
    }
    //Print results to console
    println("Actual:")
    printRDD(actual)
    //    Uncomment the code below if any doubts on correctness of the calculations above
    //    ProveTheCorrectness(data, spark)
    println("Estimate:")
    val hEstimate = GetMean(h)
    hEstimate.foreach(println)

    //Visualization
    val data1 = actual.map(r => r._2._1).collect().toList
    val data2 = hEstimate.map(r => r._2._1)
    val fileWriter = new FileWriter(new File("data/d1.txt"))
    var counter = 0
    for (elem <- data1) {
      fileWriter.write(s"$counter, $elem\n")
      counter = counter + 1
    }
    fileWriter.close()
    val fileWriter2 = new FileWriter(new File("data/d2.txt"))
    var counter2 = 0
    for (elem <- data2) {
      fileWriter2.write(s"$counter2, $elem\n")
      counter2 = counter2 + 1
    }
    fileWriter2.close()
    WageStatChart.main(Array(""))

    spark.stop()
  }

  def GetMean(h: mutable.TreeMap[String, ((Double, Int), (Double, Int))]): mutable.TreeMap[String, (Double, Double)] = {
    h.filter(r => (r._2._1._2 != 0 && r._2._2._2 != 0))
      .map(r => (r._1, (r._2._1._1 / r._2._1._2, r._2._2._1 / r._2._2._2)))

  }

  def AddToBag(h: mutable.TreeMap[String, ((Double, Int), (Double, Int))],
               toList: List[(String, (Double, Double))]): mutable.TreeMap[String, ((Double, Int), (Double, Int))] = {
    for (e <- toList) {
      if(h.contains(e._1)){
        val v = h.get(e._1).get
        h.put(e._1, (
          (v._1._1 + e._2._1, v._1._2 + 1),
          (v._2._1 + e._2._2, v._2._2 + 1)
        ))
      }
      else {
        h.put(e._1, ((e._2._1, 1), (e._2._2, 1)))
      }
    }
    h
  }

  private def calculateAvgVariance(data: RDD[(String, Double)], isSample: Int): RDD[(String, (Double, Double))] = {
    data
      .mapValues(x => (1, x, Math.pow(x, 2)))
      .reduceByKey((x, y) => (
        x._1 + y._1,
        x._2 + y._2,
        x._3 + y._3))
      .mapValues(x =>
        (x._2 / x._1, //mean
          Math.abs((x._3 / (x._1 - isSample)) - Math.pow(x._2 / (x._1 - isSample), 2)) //variance
        )
      )
      .sortByKey()
  }

  private def printRDD(actual: RDD[(String, (Double, Double))]) = {
    println(s"Occupation\tMean\tVariance")
    actual.collect()
      .map(t => s"${t._1}  ${t._2._1}  ${t._2._2}")
      .foreach(println)
  }

  private def ProveTheCorrectness(rawData: RDD[(String, Double)], session: SparkSession) = {
    //    import session.implicits._
    import session.implicits._
    val df = rawData.map(r => WageRecord(r._1, r._2)).toDF()
    val testActual = df.
      groupBy("occupation")
      .agg(
        mean("wage") as "mean",
        var_pop("wage") as "variance"
      )
      .orderBy("occupation")
      .show()
  }
}
