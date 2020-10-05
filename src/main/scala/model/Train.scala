package model

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import preprocessing.TweetPreprocess.preprocessTweet
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}

class Train {
  val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()

  import session.sqlContext.implicits._

  val training = session.read.format("csv").load("src/main/sourses/training.csv").
    toDF("target", "ids", "data", "flag", "user", "text")
    .drop("ids").drop("data").drop("flag").drop("user")

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
    .setMinCount(5)


  val model = word2Vec.fit(trainingTransformed)
  // Save and load model
  model.save("myWord2Vec")

  trainingTransformed = model.transform(trainingTransformed)
  trainingTransformed.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
  }

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setFeaturesCol("result")
    .setLabelCol("target")
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(training)
  lrModel.save("myLR")

  //  todo check
  val gbTree = new GBTClassifier()
    .setFeaturesCol("result")
    .setLabelCol("target")
    .setMaxIter(10)
    .setFeatureSubsetStrategy("auto")
    .fit(trainingTransformed)

  gbTree.save("myGBTree")

}
