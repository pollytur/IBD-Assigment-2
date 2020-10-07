import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import preprocessing.TweetPreprocess
import streaming.Streamer


object Main extends App {

  val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()
  val sqlContext = session.sqlContext

  val vectorizer = Word2VecModel.load("myWord2Vec")
  val model = LogisticRegressionModel.load("myLR")

  val (stream, context) = Streamer.stream(session.sparkContext)


  val coder2: Seq[String] => String = (arg: Seq[String]) => {
    arg.mkString(" ")
  }
  val trans = udf(coder2)

  stream.foreachRDD(rdd => {
    import session.sqlContext.implicits._

    val result = rdd
      .map((str: String) => TweetPreprocess.preprocessTweet(str).split(" "))
      .filter(_.length > 0)
      .toDF("textTransformed")
      .transform((value: Dataset[Row]) => vectorizer.transform(value))
      .transform((value: Dataset[Row]) => model.transform(value))
      .transform((value: Dataset[Row]) => value.select("textTransformed", "prediction"))
      .withColumn("textTransformed", trans(col("textTransformed")))
      .filter((row: Row) => row.getAs[String](0).length > 0)

    if (!result.rdd.isEmpty()) {
      println("IN")
      result.coalesce(1).write.mode(SaveMode.Append).csv("output")
    }

  })


  context.start()
  context.awaitTermination()

  def createDfForTweet(tweet: String): DataFrame = {
    session.createDataFrame(Seq(
      tweet.split(" ")
    ).map(Tuple1.apply)).toDF("textTransformed")
  }
}