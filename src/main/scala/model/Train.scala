package model

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import preprocessing.TweetPreprocess.preprocessTweet

class Train {
  val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()

  import session.sqlContext.implicits._

  val training = session.read.format("csv").load("src/main/sourses/training.csv").
    toDF("target", "ids", "data", "flag", "user", "text")

  var trainingTransformed = training.select("text").map {
    case Row(string_val: String) => (string_val, preprocessTweet(string_val).split(" "))
  }.toDF("text", "textTransformed")

  //  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

  val remover = new StopWordsRemover()
    .setInputCol("textTransformed")
    .setOutputCol("afterStopWordsRemoval")


  trainingTransformed = remover.transform(trainingTransformed)

  //  one tweet is 280 symbols, on average raw tweet contains 15 words
  val word2Vec = new Word2Vec()
    .setInputCol("afterStopWordsRemoval")
    .setOutputCol("result")
    .setVectorSize(15)
    .setMinCount(0)


  val model = word2Vec.fit(trainingTransformed)

  val result = model.transform(trainingTransformed)
  result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
  }

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(training)
}
