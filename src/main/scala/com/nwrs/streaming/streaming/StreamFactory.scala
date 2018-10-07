package com.nwrs.streaming.streaming

import java.util.{Collections, Properties}

import com.nwrs.streaming.twitter.{Tweet, TwitterEntity}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object StreamFactory {
  lazy val log:Logger = LoggerFactory.getLogger(StreamFactory.getClass)

  def createStream(env: StreamExecutionEnvironment, props: TweetStreamProps) = props.source match {
    case TweetStreamSource.Kafka => StreamFactory.kafkaTweetStream(env, props)
    case TweetStreamSource.Direct => StreamFactory.twitterEndpointTweetStream(env, props)
  }

  /**
    * Create a stream from incoming Avro encoded Tweets over Kafka
    *
    */
  def kafkaTweetStream(env:StreamExecutionEnvironment, props: TweetStreamProps):DataStream[Tweet] = {
    log.info(s"Creating stream from Kafka. kafkaServers=${props.kafkaServers} kafkaTopic=${props.kafkaTopic}")
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", props.kafkaServers)
    env.addSource(new FlinkKafkaConsumer010[Tweet](props.kafkaTopic, new Tweet.TweetDeserializationSchema(), kafkaProperties).setStartFromLatest())
  }

  /**
    * Create a stream of Tweets from a direct connection to Twitter
    *
    * @return
    */
  def twitterEndpointTweetStream(env:StreamExecutionEnvironment, props: TweetStreamProps):DataStream[Tweet] = {
    log.info(s"Creating stream from direct Twitter connection.")

    // pull out required properties here to avoid props object being serialized into closure
    val langs:java.util.List[String] = if (!props.langs.isEmpty)
      props.langs.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val searchTerms:java.util.List[String] = if (!props.searchTerms.isEmpty)
      props.searchTerms.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val users:java.util.List[java.lang.Long] = if (!props.users.isEmpty)
      props.users.split(",").toSeq.map(u => Long.box(u.toLong)).asJava
    else
      Collections.emptyList()


    val ts = new TwitterSource(props.twitterProps)
    ts.setCustomEndpointInitializer(new TwitterSource.EndpointInitializer with Serializable {
      override def createEndpoint(): StreamingEndpoint = {
        val ep = new StatusesFilterEndpoint()
        if (!langs.isEmpty) ep.languages(langs)
        if (!searchTerms.isEmpty) ep.trackTerms(searchTerms)
        if (!users.isEmpty) ep.followings(users)
        ep
      }
    })

    // convert from stream of json strings to stream of tweet objects, ignore non tweet entities (limitation notices etc...)
    env.addSource(ts)
      .map[Option[TwitterEntity]](TwitterEntity.fromJson(_))
      .filter(te => te.nonEmpty && te.get.isInstanceOf[Tweet])
      .map(_.get.asInstanceOf[Tweet])
  }
}
