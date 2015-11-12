/**
  * Created by ehaval on 11/3/15.
  */


import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf

object ImportData {

  def main(args:Array[String]) = {
    // initialise spark contexts
    val conf = new SparkConf().setAppName("DataPipeline")
    val ssc = new StreamingContext(conf,Seconds(2))
    val inputTopic = "inputqueue"
    val group = "none"
    val kafkaStream = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group", Map(inputTopic -> 1))
    ssc.start()
    ssc.awaitTerminationOrTimeout(75000)


  }
}