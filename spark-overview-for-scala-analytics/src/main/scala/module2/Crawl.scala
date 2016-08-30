package module2

import org.apache.spark.SparkContext
import util.Files

/**
  * Created by david on 30/08/16.
  */
object Crawl {
  def main(args: Array[String]) {
    val separator = "data/enron-spam-ham/*"
    val inPath = "data/all-shakespeare.txt"
    val outPath = "output/crawl"

    Files.rmrf(outPath)

    val sc = new SparkContext("local[*]", "Crawl")

    try {
      val files_contents = sc.wholeTextFiles(inPath)
        .map {
          case (id, text) =>
            val lastSep = id.lastIndexOf(separator)
            val id2 = if (lastSep < 0) id.trim else id.substring(lastSep + 1, id.length).trim
            val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
            (id2, text2)

        }

      println(s"Writing output to: $outPath")
      files_contents.saveAsTextFile(outPath)

    } finally {
      sc.stop
    }
  }
}
