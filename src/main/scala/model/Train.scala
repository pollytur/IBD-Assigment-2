package model

import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec, Word2VecModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession
import preprocessing.TweetPreprocess.preprocessTweet
import org.apache.spark.sql.functions.{rand, udf}
import org.apache.spark.sql.functions._

object Train {
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

  val remover = new StopWordsRemover()
    .setInputCol("text")
    .setOutputCol("textTransformed")

  //    https://stackoverflow.com/questions/30219592/create-new-column-with-function-in-spark-dataframe
  val coder: String => Array[String] = (arg: String) => {
    preprocessTweet(arg).split(" ")
  }
  val sqlfunc = udf(coder)

  def train() = {
    println("TRAIN STARTED")
    val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()



    //    val toInt = udf[Int, String](x => x.toInt>0 ? 1 | 0)

    val trainingInit = session.read.format("csv").load("src/main/sourses/initial_dataset.csv")
      .toDF("target", "ids", "data", "flag", "user", "text")
      .drop("ids").drop("data").drop("flag").drop("user").orderBy(rand())

    val split = trainingInit.randomSplit(Array(0.9, 0.1))
    //    split(0).write.csv("src/main/sourses/train_dataset.csv")
    var test = split(1)
    //    test = test.withColumn("target", toInt(col("target")))
    //    test.write.csv("src/main/sourses/test_dataset.csv")
    val training = split(0)

    println("CVS READ")

    var trainingTransformed = training.withColumn("text", sqlfunc(col("text")))

    println("CVS PREPROCESSING")

    trainingTransformed = remover.transform(trainingTransformed)
    println("REMOVER DONE")

    //  one tweet is 280 symbols, on average raw tweet contains 15 words
    val word2Vec = new Word2Vec()
      .setInputCol("textTransformed")
      .setOutputCol("result")
      .setVectorSize(100)
      .setMinCount(5)


    val model = word2Vec.fit(trainingTransformed)
    // Save and load model

    println("WORD 2 VEC DONE")
    model.save("myWord2Vec-100")
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

    //    merged.write.csv("after_word2vec_transformed_dataset.csv")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(trainingTransformed)
    println("LOGISTIC REGRESSION DONE")
    lrModel.save("myLR-100")
    println("LOGISTIC REGRESSION SAVED")

    //  todo check
    val gbTree = new GBTClassifier()
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")
      .fit(trainingTransformed)

    println("GRADIENT BOOSTING TREE DONE")

    gbTree.save("myGBTree-100")

    println("GRADIENT BOOSTING TREE DONE")

    val lsvc = new LinearSVC()
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setMaxIter(10)
      .setRegParam(0.1)

    // Fit the model
    val lsvcModel = lsvc.fit(trainingTransformed)

    lsvcModel.save("LinearSVC-100")

    val rf = new RandomForestClassifier()
      .setFeaturesCol("result")
      .setLabelCol("target")
      .setNumTrees(10)
      .setFeatureSubsetStrategy("auto")
      .fit(trainingTransformed)

    rf.save("RandomForest-100")
  }

  def evaluationSummary(evaluator: MulticlassMetrics): Unit = {
    println(s"Confusion matrix : ${evaluator.confusionMatrix}")
    println(s"For negative(0) label: F-score ${evaluator.fMeasure(0)}, recall: ${evaluator.recall(0)}, " +
      s"precision: ${evaluator.precision(0)}")
    println(s"For positive(1) label: F-score ${evaluator.fMeasure(1)}, recall: ${evaluator.recall(1)}, " +
      s"precision: ${evaluator.precision(1)}")
  }

  def modelEvaluation(): Unit = {
    val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()

    import session.sqlContext.implicits._

    var valid = session.read.format("csv").load("src/main/sourses/test_dataset.csv")
      .toDF("target", "text")

    valid = valid.withColumn("target", toInt(col("target")))
    valid = valid.withColumn("text", sqlfunc(col("text")))
    valid = remover.transform(valid)

    valid = Word2VecModel.load("myWord2Vec-100").transform(valid)

    val lrResults = LogisticRegressionModel.load("myLR-100").transform(valid).drop("text")
      .drop("result").drop("textTransformed").drop("rawPrediction")
      .drop("probability").select("prediction", "target")
      .withColumnRenamed("target", "label")

    val gbResults = GBTClassificationModel.load("myGBTree-100").transform(valid).drop("text")
      .drop("result").drop("textTransformed").drop("rawPrediction")
      .drop("probability").select("prediction", "target")
      .withColumnRenamed("target", "label")

    val rfResults = RandomForestClassificationModel.load("RandomForest-100").transform(valid).drop("text")
      .drop("result").drop("textTransformed").drop("rawPrediction")
      .drop("probability").select("prediction", "target")
      .withColumnRenamed("target", "label")

    val lsvcResults = LinearSVCModel.load("LinearSVC-100").transform(valid).drop("text")
      .drop("result").drop("textTransformed").drop("rawPrediction")
      .drop("probability").select("prediction", "target")
      .withColumnRenamed("target", "label")

    val gbTreeEvaluator = new MulticlassMetrics(gbResults.as[(Double, Double)].rdd)
    val lrEvaluator = new MulticlassMetrics(lrResults.as[(Double, Double)].rdd)
    val rfEvaluator = new MulticlassMetrics(rfResults.as[(Double, Double)].rdd)
    val lsvcEvaluator = new MulticlassMetrics(lsvcResults.as[(Double, Double)].rdd)

    println("Summary for Gradient-boosted tree classifier")
    evaluationSummary(gbTreeEvaluator)
    println("Summary for Logistic Regression classifier")
    evaluationSummary(lrEvaluator)

    println("Summary for Random Forest classifier")
    evaluationSummary(rfEvaluator)
    println("Summary for Linear SVC classifier")
    evaluationSummary(lsvcEvaluator)


  }
}
