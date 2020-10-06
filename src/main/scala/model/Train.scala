package model

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec}
import org.apache.spark.sql.SparkSession
import preprocessing.TweetPreprocess.preprocessTweet
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.sql.functions.rand

object Train {
  def train() = {
    println("TRAIN STARTED")
    val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()

    import org.apache.spark.sql.functions._

    val coder0: String => Int = (arg: String) => {
//      arg.toInt>0 ? 1 | 0
      if (arg.toInt > 0) {
        1
      }
      else {
        0
      }
    }
    val toInt = udf(coder0)

    //    val toInt = udf[Int, String](x => x.toInt>0 ? 1 | 0)

    val trainingInit = session.read.format("csv").load("src/main/sourses/initial_dataset.csv")
      .toDF("target", "ids", "data", "flag", "user", "text")
      .drop("ids").drop("data").drop("flag").drop("user").orderBy(rand())

    val split = trainingInit.randomSplit(Array(0.9, 0.1))
    split(0).write.csv("src/main/sourses/train_dataset.csv")
    var test = split(1)
    test = test.withColumn("target", toInt(col("target")))
    test.write.csv("src/main/sourses/test_dataset.csv")
    val training = split(0)

    println("CVS READ")
    //    https://stackoverflow.com/questions/30219592/create-new-column-with-function-in-spark-dataframe
    val coder: String => Array[String] = (arg: String) => {
      preprocessTweet(arg).split(" ")
    }
    val sqlfunc = udf(coder)
    var trainingTransformed = training.withColumn("text", sqlfunc(col("text")))

    println("CVS PREPROCESSING")

    val remover = new StopWordsRemover()
      .setInputCol("text")
      .setOutputCol("textTransformed")


    trainingTransformed = remover.transform(trainingTransformed)
    println("REMOVER DONE")

    //  one tweet is 280 symbols, on average raw tweet contains 15 words
    val word2Vec = new Word2Vec()
      .setInputCol("textTransformed")
      .setOutputCol("result")
      .setVectorSize(15)
      .setMinCount(5)


    val model = word2Vec.fit(trainingTransformed)
    // Save and load model

    println("WORD 2 VEC DONE")
    model.save("myWord2Vec")
    println("WORD 2 VEC SAVED")


    trainingTransformed = model.transform(trainingTransformed)
    println("WORD 2 VEC TRANSFORMATION DONE")

    trainingTransformed = trainingTransformed.withColumn("target", toInt(col("target")))
    var merged = trainingTransformed

    val coder2: Seq[String] => String = (arg: Seq[String]) => {
      arg.mkString(" ")
    }
    val trans = udf(coder2)

    val coder3: Any => String = (arg: Any) => {
      arg.toString
    }
    val trans2 = udf(coder3)
    merged = merged.withColumn("result", trans2(col("result")))
    merged = merged.withColumn("text", trans(col("text")))
    merged = merged.withColumn("textTransformed", trans(col("textTransformed")))

    merged.write.csv("after_word2vec_transformed_dataset.csv")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(trainingTransformed)
    println("LOGISTIC REGRESSION DONE")
    lrModel.save("myLR")
    println("LOGISTIC REGRESSION SAVED")

    //  todo check
    val gbTree = new GBTClassifier()
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")
      .fit(trainingTransformed)

    println("GRADIENT BOOSTING TREE DONE")

    gbTree.save("myGBTree")

    println("GRADIENT BOOSTING TREE DONE")
  }
}
