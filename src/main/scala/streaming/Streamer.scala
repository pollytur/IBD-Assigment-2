package streaming

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object Streamer {
  def stream(context: SparkContext): (DStream[String], StreamingContext) = {
    val ssc = new StreamingContext(context, Seconds(10))

    val lines = ssc.socketTextStream("10.90.138.32", 8989)

    (lines, ssc)
  }

  def test(session: SparkSession): Unit = {
    val (stream, context) = Streamer.stream(session.sparkContext)

    stream.print(1)

    context.start()
    context.awaitTermination()
  }
}


