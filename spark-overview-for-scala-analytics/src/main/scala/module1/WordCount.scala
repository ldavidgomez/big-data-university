package module1

import org.apache.spark.SparkContext
import util.Files

/**
  * Created by david on 30/08/16.
  */
object WordCount {
  def main(args: Array[String]) {
    val inPath = "data/all-shakespeare.txt"
    val outPath = "output/word_count1"

    Files.rmrf(outPath)

    val sc = new SparkContext("local[*]", "WordCount")

    try {
      val input = sc.textFile(inPath)
      val wc = input
        .map(_.toLowerCase)
        .flatMap(text => text.split("""\W+"""))
        .groupBy(word => word)
        .mapValues(group => group.size)

      println(s"Writing output to: $outPath")
      wc.saveAsTextFile(outPath)
      printMsg("Enter any key to finish the job...")
      Console.in.read()
    } finally {
      sc.stop
    }
  }

  private def printMsg(m: String) = {
    println("")
    println(m)
    println("")
  }
}
