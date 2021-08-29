
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


object WageStatWithDF {


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
    val actual = calculateAvgVariance(data, spark, false).cache()
    println("Actual:")
    actual.toDF().show()

    val sample = data.sample(false, 0.25).cache()
    var h = new mutable.TreeMap[String, ((Double, Int), (Double, Int))]

    for (a <- 1 to 50) {
      val resample = sample.sample(true, 1)
      val estimation = calculateAvgVariance(resample, spark, true)
      h = AddToBag(h, estimation.rdd.map(r =>
        (r(0).asInstanceOf[String], (r(1).asInstanceOf[Double], r(2).asInstanceOf[Double])))
        .collect().toList
      )
    }

    println("Estimate:")
    val hEstimate = GetMean(h)
    hEstimate.foreach(println)

    spark.stop()
  }

  private def calculateAvgVariance(rawData: RDD[(String, Double)], session: SparkSession, isSample: Boolean) = {
    //    import session.implicits._
    import session.implicits._
    val df = rawData.map(r => WageRecord(r._1, r._2)).toDF()
    if (isSample) {
      df.
        groupBy("occupation")
        .agg(
          mean("wage") as "mean",
          variance("wage") as "variance"
        )
        .orderBy("occupation")
    }

    else {
      df.
        groupBy("occupation")
        .agg(
          mean("wage") as "mean",
          var_pop("wage") as "variance"
        )
        .orderBy("occupation")
    }
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

}
