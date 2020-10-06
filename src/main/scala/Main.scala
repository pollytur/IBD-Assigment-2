import model.Train
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import preprocessing.TweetPreprocess
import streaming.Streamer


object Main extends App {
  //    Train.modelEvaluation()
  val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()

  val vectorizer = Word2VecModel.load("myWord2Vec")
  val model = LogisticRegressionModel.load("myLR")

  val (stream, context) = Streamer.stream(session.sparkContext)

  stream.map((rawTweet: String) => TweetPreprocess.preprocessTweet(rawTweet))
    .map((tweet: String) => createDfForTweet(tweet))
    .map((frame: DataFrame) => vectorizer.transform(frame))
    .map((frame: DataFrame) => model.transform(frame))
    .saveAsTextFiles("output")

  context.start()
  context.awaitTermination()

  def createDfForTweet(tweet: String): DataFrame = {
    session.createDataFrame(Seq(
      tweet.split(" ")
    ).map(Tuple1.apply)).toDF("textTransformed")
  }
}
