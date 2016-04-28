import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.cloudera.spark.streaming.kafka.KafkaWriter._


object Main{
  def main(args: Array[String]) {
    val master = "spark://cruncher:7077"
    val appName = "SS7MLPreprocess"

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.cores.max", "8")

    // Elasticsearch configuration.
    conf.set("es.index.auto.create", "true")
    val topics = Set("ss7-raw-input")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //Kafka input configuration
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")

    //Kafka output configuration
    val kafkaOutParams = new Properties()
    kafkaOutParams.put("bootstrap.servers","localhost:9092")
    kafkaOutParams.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    kafkaOutParams.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val kafkaSink = sc.broadcast(KafkaSink(kafkaOutParams))

    //Used to create timing features
    var prevLocUpdate: LocationUpdate = LocationUpdate()

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
                if (prevLocUpdate.prevLac == 0.0)
                  travelDistance(newLac, newLac)
                else
                  travelDistance(newLac, prevLocUpdate.prevLac)

              prevLocUpdate = LocationUpdate(timeEpoch, byteLength, travelDist, lastUpdate, newLac)

              val preProcessed = Map[String, String](
                "timeEpoch" -> timeEpoch.toString,
                "byteLength" -> byteLength.toString,
                "lastUpdate" -> lastUpdate.toString,
                "travelDist" -> travelDist.toString,
                "newLac" -> newLac.toString
              )

              //Store preprocessed values in elasticsearch for further analysis and visualization
              val preProcRDD = sc.makeRDD(Seq(preProcessed))
              preProcRDD.saveToEs("ss7-preprocessed/preprocessed")

              //Send preprocessed data on Kafka for ML analysis
              val kafkaOutString = timeEpoch.toString + "," + byteLength.toString + "," + lastUpdate.toString + "," + travelDist.toString + "," + newLac.toString
              kafkaSink.value.send("ss7-preprocessed", kafkaOutString)
            }
          }
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }

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
