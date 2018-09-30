package com.nwrs.streaming.streaming

import java.util.Properties

import com.nwrs.streaming.twitter.{Tweet, TwitterEntity}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


class TerminateStreamException extends Exception

object StreamFactory {
  lazy val log:Logger = LoggerFactory.getLogger(StreamFactory.getClass)

  /**
    * Create a stream from incoming Avro encoded Tweets over Kafka
    *
    * @param props
    * @return
    */
  def kafkaTweetStream(env:StreamExecutionEnvironment, props:Properties):DataStream[Tweet] = {
    val servers = props.getProperty("kafkaServers","localhost:9092")
    val topic = props.getProperty("kafkaTopic","tweets")
    log.info(s"Creating stream from Kafka. kafkaServers=$servers kafkaTopic=$topic")
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", servers)
    env.addSource(new FlinkKafkaConsumer010[Tweet](topic, new Tweet.TweetDeserializationSchema(), kafkaProperties).setStartFromLatest())
  }

  /**
    * Create a stream of Tweets from a direct connection to Twitter
    *
    * @param env
    * @param props
    * @return
    */
  def twitterEndpointTweetStream(env:StreamExecutionEnvironment, props:Properties):DataStream[Tweet] = {
    log.info(s"Creating stream from direct Twitter connection.")

    val ts = new TwitterSource(props)
    ts.setCustomEndpointInitializer(new TwitterSource.EndpointInitializer with Serializable {
      override def createEndpoint(): StreamingEndpoint = {
        val ep = new StatusesFilterEndpoint()
        if (props.getProperty("languages")!=null)
          ep.languages(props.getProperty("languages").split(",").toSeq.asJava)
        if (props.getProperty("searchTerms")!=null)
          ep.trackTerms(props.getProperty("searchTerms").split(",").toSeq.asJava)
        if (props.getProperty("users")!=null)
          ep.followings(props.getProperty("users").split(",").toSeq.map(u => Long.box(u.toLong)).asJava)
        ep
      }
    })

    // convert from stream of json strings to stream of tweet objects, ignore non tweet entities (limitation notices etc...)
    env.addSource(ts)
      .map[Option[TwitterEntity]](TwitterEntity.fromJson(_))
      .filter(te => te.nonEmpty && te.get.isInstanceOf[Tweet])
      .map(_.get.asInstanceOf[Tweet])
  }

  def parseWindowTime(time:String):Time = {
    val timeRegex = "([0-9]+)([sm]*)".r
    time.trim match {
      case timeRegex(t,sm) => {
        sm match {
          case "m" => Time.minutes(t.toLong)
          case "s" => Time.seconds(t.toLong)
        }
      }
    }
  }

}
