import org.apache.spark.sql.SparkSession
import preprocessing.TweetPreprocess.preprocessTweet

object Main extends App {
  //  println(preprocessTweet("@model i love8 u take with u all the meu in 6291 :) "))
  val session = SparkSession.builder().appName("app_name").master("local").getOrCreate()
  println("done")
  val training = session.read.format("csv").load("src/main/sourses/training.csv").rdd
  println("done")
  println(training.id)
}
