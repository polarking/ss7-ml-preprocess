import java.util.{Date, Properties}

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.cloudera.spark.streaming.kafka.KafkaWriter._

object Main{
  /**
    * Main entry point to the application.
    * @param args The command line arguments.
    */
  def main(args: Array[String]) {
    val master = args(0)
    val appName = "SS7MLPreprocess"

    val user = args(1)
    val pass = args(2)

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)

    // Elasticsearch configuration.
    conf.set("es.resource", "ss7-ml-preprocessed/preprocessed")
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user", user)
    conf.set("es.net.http.auth.pass", pass)

    val topics = Set("ss7-raw-input") //Topic name for Kafka.

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //Kafka input configuration
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")

    //Kafka output configuration
    val kafkaOutParams = new Properties()
    kafkaOutParams.put("bootstrap.servers","localhost:9092")
    kafkaOutParams.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    kafkaOutParams.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val kafkaSink = sc.broadcast(KafkaSink(kafkaOutParams)) //Sink used to send messages to Kafka.

    //Used to create timing features
    var prevLocUpdate: LocationUpdate = LocationUpdate()

    //Increment to identify feature set
    var label = 0

    //Stream messages from Kafka: network capture
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
      .flatMap(_._2.split("\n")).foreachRDD(ss7Record => {
        val ss7Input = ss7Record.collect()
        ss7Input.foreach(input => {
          val line = input.split(",")
          val mapMessage = line(4)

          //Only interested in updateLocation requests
          if(mapMessage.contains("invoke updateLocation") && !mapMessage.contains("returnResultLast")) {
            val imsi = line(13)

            //Looking for location updates for the VIP subscriber
            if(imsi == "24201111111110") {
              val timeEpoch = line(0).trim.toDouble
              val byteLength = line(3).trim.toDouble
              val lastUpdate = timeEpoch - prevLocUpdate.timeEpoch
              val newLac = LAC.lacDecode(line(15).trim)
              val travelDist =
                if (prevLocUpdate.prevLac == 0.0) //If it's the first read updateLocation.
                  travelDistance(newLac, newLac)
                else
                  travelDistance(newLac, prevLocUpdate.prevLac)

              prevLocUpdate = LocationUpdate(timeEpoch, byteLength, travelDist, lastUpdate, newLac)

              val preProcessedDate = Map(
                "timeEpoch" -> new Date(timeEpoch.toInt * 1000L),
                "byteLength" -> byteLength.toInt,
                "newLac" -> newLac,
                "lastUpdate" -> lastUpdate,
                "travelDist" -> travelDist,
                "label" -> label
              )

              //Store preprocessed values in elasticsearch for further analysis and visualization
              val preProcRDD = sc.makeRDD(Seq(preProcessedDate))
              preProcRDD.saveToEs("ss7-ml-preprocessed/preprocessed")

              //Send preprocessed data on Kafka for ML analysis
              val kafkaOutString = label.toString + "," + byteLength.toString + "," + lastUpdate.toString + "," + travelDist.toString + "," + newLac.toString
              kafkaSink.value.send("ss7-preprocessed", kafkaOutString)

              label += 1
            }
          }
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Provides a metric for the distance traveled between two LACs by the subscriber.
    * @param prevLac The previous known location.
    * @param newLac The new known location.
    * @return Distance traveled by the subscriber.
    */
  def travelDistance(prevLac: Int, newLac: Int): Int = {
    var pLac = prevLac
    var nLac = newLac

    if(pLac > nLac) {
      val t = pLac
      pLac = nLac
      nLac = t
    }

    LAC.getDistance(pLac.toString + "-" + nLac.toString)
  }
}
