import model.Train
import org.apache.spark.ml.classification.{GBTClassificationModel, LinearSVCModel, LogisticRegressionModel, RandomForestClassificationModel}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import preprocessing.TweetPreprocess
import streaming.Streamer


object Main extends App {
  /* first argument mode - 0 is no training, 1 is to train
   second argument is word2vec output size (15 or 100 for no training)
   third argument is classificator choice (0-logistic regression, 1, gbtree, 2- random forest, 3 - liner svc)
   forth is path to save/load word to vec
   fifth is path to save/load classificator
   sixth arg to save output
   */
  var problems = false

  if (args.length != 6) {
    problems = true
    println("Incorrect number of arguments, should be 5. " +
      "\n first argument mode - 0 is no training, 1 is to train" +
      "\n second argument is word2vec output size (15 or 100 for no training)" +
      "\n third argument is classificator choice (0-logistic regression, 1, gbtree, 2- random forest, 4 - liner svc)" +
      "\n forth is path to save/load word to vec " +
      "\n fifth is path to save/load classificator" +
      "\n sixth argument is directory tp save output" +
      "\n Please restart the program with correct argument passed")
  }
  else if (args(2).length > 1 || !args(2).charAt(0).isDigit || args(2).toInt > 3) {
    problems = true
    println("the third argument should be in range [0,3]")
  }
  else if (args(0).equals("0")) {
    if (!args(1).forall(_.isDigit) || (!args(1).equals("15") && !args(1).equals("100"))) {
      problems = true
      println("The second argument should be integer: either 100 or 15")
    }
    if (args(2).equals("2") || args(2).equals("3")) {
      if (args(1).equals("15")) {
        problems = true
        println("Only vec size of 100 available for random forest and Linear SVC")
      }
    }
  }
  else {
    if (!args(1).forall(_.isDigit)) {
      problems = true
      println("The second argument should be integer")
    }
    else {
      Train.train(size = args(1).toInt, mode = args(2), toSave = Array(args(3), args(4)))
    }
  }

  if (!problems) {
    val session = SparkSession.builder().appName("app_name").master("local[2]").getOrCreate()
    val sqlContext = session.sqlContext
    val (stream, context) = Streamer.stream(session.sparkContext)

    val vectorizer = Word2VecModel.load(args(3))

    val coder2: Seq[String] => String = (arg: Seq[String]) => {
      arg.mkString(" ")
    }
    val trans = udf(coder2)
    val preprocess: String => Seq[String] = (str: String) => {
      TweetPreprocess.preprocessTweet(str).split(" ")
    }
    val processTweet = udf(preprocess)

    args(2) match {
      case "0" => {
        val model = LogisticRegressionModel.load(args(4))

        stream.foreachRDD(rdd => {
          import session.sqlContext.implicits._

          val result = rdd
            .toDF("initialTweet")
            .withColumn("textTransformed", processTweet(col("initialTweet")))
            .transform((value: Dataset[Row]) => vectorizer.transform(value))
            .transform((value: Dataset[Row]) => model.transform(value))
            .transform((value: Dataset[Row]) => value.select("initialTweet", "prediction"))
            .filter((row: Row) => row.getAs[String](0).length > 0)

          if (!result.rdd.isEmpty()) {
            println("IN")
            result.coalesce(1).write.mode(SaveMode.Append).csv(args(5))
          }

        })
      }
      case "1" => {
        val model = GBTClassificationModel.load(args(4))
        stream.foreachRDD(rdd => {
          import session.sqlContext.implicits._

          val result = rdd
            .toDF("initialTweet")
            .withColumn("textTransformed", processTweet(col("initialTweet")))
            .transform((value: Dataset[Row]) => vectorizer.transform(value))
            .transform((value: Dataset[Row]) => model.transform(value))
            .transform((value: Dataset[Row]) => value.select("initialTweet", "prediction"))
            .filter((row: Row) => row.getAs[String](0).length > 0)

          if (!result.rdd.isEmpty()) {
            println("IN")
            result.coalesce(1).write.mode(SaveMode.Append).csv(args(5))
          }

        })
      }
      case "2" => {
        val model = RandomForestClassificationModel.load(args(4))
        stream.foreachRDD(rdd => {
          import session.sqlContext.implicits._

          val result = rdd
            .toDF("initialTweet")
            .withColumn("textTransformed", processTweet(col("initialTweet")))
            .transform((value: Dataset[Row]) => vectorizer.transform(value))
            .transform((value: Dataset[Row]) => model.transform(value))
            .transform((value: Dataset[Row]) => value.select("initialTweet", "prediction"))
            .filter((row: Row) => row.getAs[String](0).length > 0)

          if (!result.rdd.isEmpty()) {
            println("IN")
            result.coalesce(1).write.mode(SaveMode.Append).csv(args(5))
          }

        })
      }
      case "3" => {
        val model = LinearSVCModel.load(args(4))
        stream.foreachRDD(rdd => {
          import session.sqlContext.implicits._

          val result = rdd
            .toDF("initialTweet")
            .withColumn("textTransformed", processTweet(col("initialTweet")))
            .transform((value: Dataset[Row]) => vectorizer.transform(value))
            .transform((value: Dataset[Row]) => model.transform(value))
            .transform((value: Dataset[Row]) => value.select("initialTweet", "prediction"))
            .filter((row: Row) => row.getAs[String](0).length > 0)

          if (!result.rdd.isEmpty()) {
            println("IN")
            result.coalesce(1).write.mode(SaveMode.Append).csv(args(5))
          }

        })
      }
    }
    context.start()
    context.awaitTermination()

    def createDfForTweet(tweet: String): DataFrame = {
      session.createDataFrame(Seq(
        tweet
      ).map(Tuple1.apply)).toDF("textTransformed")
    }
  }


}