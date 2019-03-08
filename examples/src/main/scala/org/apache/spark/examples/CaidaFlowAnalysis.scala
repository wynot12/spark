import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object CaidaFlowAnalysis {
  val pattern: Regex = " *\\d+ +[0-9.]+ +([0-9.]+) -> ([0-9.]+) +.*Len=(\\d+)".r
  def mapper0(line: String): (String, (String, Long)) = {
    val matched = pattern.findFirstMatchIn(line).get
    return (matched.group(2), (matched.group(1), matched.group(3).toLong))
  }
  def mapper1(line: String): (String, (String, Long)) = {
    val matched = pattern.findFirstMatchIn(line).get
    return (matched.group(1), (matched.group(2), matched.group(3).toLong))
  }
  def stdev(data: Iterable[(String, Long)]): Double = {
    var num: Long = 0
    var sum: Long = 0
    var squareSum: Long = 0
    data.map(x => x._2).foreach(x => {
      num += 1
      sum += x
      squareSum += (x * x)
    })
    if (num == 0) {
      return Double.NaN
    }
    val average: Double = sum.toDouble / num
    val squareAverage: Double = squareSum.toDouble / num
    return Math.sqrt(squareAverage - (average * average))
  }
  def getDouble(data: Iterable[Double]): Double = {
    for (datum <- data) {
      return datum
    }
    return Double.NaN
  }
  def main(args: Array[String]): Unit = {
    // args
    val inputPath0 = args(0)
    val inputPath1 = args(1)
    val outputPath = args(2)

    val sc = new SparkContext()
    val start = System.currentTimeMillis()

    val in0: RDD[(String, Double)] = sc.textFile(inputPath0)
      .filter(line => pattern.findFirstMatchIn(line).isDefined)
      .map(mapper0)
      .groupByKey(107)
      .map(x => (x._1, stdev(x._2)))
    val in1: RDD[(String, Double)] = sc.textFile(inputPath1)
      .filter(line => pattern.findFirstMatchIn(line).isDefined)
      .map(mapper1)
      .groupByKey(45)
      .map(x => (x._1, stdev(x._2)))
    val joined: RDD[(String, (Iterable[Double], Iterable[Double]))] = in0.cogroup(in1, 10)
    val toWrite: RDD[String] = joined.map(x => x._1 + "," + getDouble(x._2._1).toString + "," + getDouble(x._2._2).toString)
    toWrite.saveAsTextFile(outputPath)

    // DONE
    println("*******END*******")
    println("JCT(ms): " + (System.currentTimeMillis()-start))
  }
}
