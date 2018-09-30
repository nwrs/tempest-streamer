package com.nwrs.streaming

import java.util.Properties

import com.nwrs.streaming.analytics._
import org.apache.flink.streaming.api.scala._
import com.nwrs.streaming.analytics.DataStreamImplicits._
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.StreamFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.io.Source

object MainApp {

  def getProps(args:Array[String]):Properties = {
    val params:ParameterTool = ParameterTool.fromArgs(args)
    val props = params.has("configFile") match {
      case true => ParameterTool.fromPropertiesFile (params.get ("configFile") ).getProperties
      case _ => new Properties
    }
    props.putAll(params.getProperties)
    props
  }

  def logProps(props:Properties):Unit = {
    props.stringPropertyNames().asScala.foreach(p => {
      val value = if (p.toLowerCase.contains("secret")) "xxxxxxxxxxxxx" else props.getProperty(p)
      log.info(s"$p = $value")
    })
  }

  lazy val log:Logger = LoggerFactory.getLogger(MainApp.getClass)

  def main(args : Array[String]): Unit = {
    log.info("Starting Tempest Tweet Streamer...")

    // TODO - Complete rest service
    // Start REST service
    // Future { RestService.startAndAwait(9898) } (ExecutionContext.global)

    // load params and override if required
    implicit val props:Properties = getProps(args)
    logProps(props)
    if (props.size() == 0) {
      Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("options.txt")).getLines().foreach(println)
      System.exit(0)
    }
    val elasticUrl = props.getProperty("elasticUrl", "localhost:9200")
    val timeWindowProp = props.getProperty("windowTime", "60s")
    val timeWindow = StreamFactory.parseWindowTime(timeWindowProp)
    val parallelism = props.getProperty("parallelism", "1").toInt

    // Ensure Elasticsearch cluster available, provisioning indexes if required
    new ElasticUtils(elasticUrl).ensureElasticSearchCluster()

    // create stream
    log.info(s"Streams will window every $timeWindowProp with parallelism of $parallelism")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,5000))

    val stream = props.getProperty("tweetSource", "direct").toLowerCase match {
      case "kafka" => StreamFactory.kafkaTweetStream(env, props)
      case "direct" => StreamFactory.twitterEndpointTweetStream(env, props)
      case ts => throw new Exception(s"Unknown tweet source '$ts' select one of 'kafka' or 'direct'")
    }

    // add results to stream
    Seq(HashtagResult,
      TweetCountResult,
      ProfileTopicResult,
      ProfileBigramsResult,
      InfluencersResult,
      GenderResult,
      SourceResult,
      GeoLocationResult,
      RetweetResult,
      LinkResult).foreach(r => {
        log.info(s"Adding result to stream: ${r.name()} - ${r.description()}")
        stream.addPipelineResult(r, timeWindow, parallelism)
      }
    )

    // start streaming
    env.execute("Tempest Twitter Streaming")
  }

}
