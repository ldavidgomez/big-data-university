package module2

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import util.{Files, StopWords}

/**
  * Created by david on 30/08/16.
  */
object InvertedIndexImprovedStopWords {
  def main(args: Array[String]) {
    val inPath = "output/crawl"
    val outPath = "output/inverted-index"

    Files.rmrf(outPath)

    val sc = new SparkContext("local[*]", "Inverted Index")
    val stopWords: Broadcast[Set[String]] = sc.broadcast(StopWords.words)

    try {
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(inPath).map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          ("", "")
      }

      input
        .flatMap {
          case (path, text) =>
            text.trim.split("""[^\w']""")
              .map(word => ((word, path), 1))
        }
        .filter {
          case ((word, _), _) =>
            stopWords.value.contains(word) == false
        }
        .reduceByKey {
          (count1, count2) => count1 + count2
        }
        .map {
          case ((word, path), n) => (word, (path, n))
        }
        .groupByKey
        .sortByKey(ascending = true)
        .mapValues { iterable =>
          val vect = iterable.to[Vector].sortBy {
            case (path, n) => (-n, path)
          }
          vect.mkString(", ")
        }
        .saveAsTextFile(outPath)

    } finally {
      println("""...""")
      Console.in.read()
      sc.stop
    }
  }
}
