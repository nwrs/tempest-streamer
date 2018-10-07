package com.nwrs.streaming.streaming

import java.util.Properties
import com.nwrs.streaming.streaming.TweetStreamSource.TweetStreamSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object TweetStreamSource extends Enumeration {
  type TweetStreamSource = Value
  val Kafka, Direct = Value
}

case class TweetStreamProps(source:TweetStreamSource=TweetStreamSource.Direct,
                            parallelism:Int=1,
                            windowTime:Time,
                            kafkaServers:String="",
                            kafkaTopic:String="",
                            searchTerms:String="",
                            langs:String="",
                            users:String="",
                            elasticUrl:String="",
                            twitterProps:Properties=new Properties)


object TweetStreamProps {
  lazy val log:Logger = LoggerFactory.getLogger(TweetStreamProps.getClass)

  def getProps(args:Array[String]):Option[TweetStreamProps] = {

    // extract cmd line params, overloading from config file is required
    val params:ParameterTool = ParameterTool.fromArgs(args)
    val props = params.has("configFile") match {
      case true => ParameterTool.fromPropertiesFile (params.get ("configFile") ).getProperties
      case _ => new Properties
    }
    props.putAll(params.getProperties)

    if (props.size > 0) {
      // log props
      props.stringPropertyNames().asScala.foreach(p => {
        val value = if (p.toLowerCase.contains("secret")) "xxxxxxxxxxxxx" else props.getProperty(p)
        log.info(s"$p = $value")
      })

      // extract twitter API specific props
      val twitterProps = new Properties
      props.stringPropertyNames
        .asScala
        .filter( (p:String) => p.startsWith("twitter"))
        .map(k => twitterProps.setProperty(k, props.getProperty(k)))

      // tweet source
      val source =props.getProperty("tweetSource", "direct").toLowerCase match {
        case "kafka" => TweetStreamSource.Kafka
        case "direct" => TweetStreamSource.Direct
      }

      // create props obj
      Some(TweetStreamProps(source,
        props.getProperty("parallelism", "1").toInt,
        parseWindowTime(props.getProperty("windowTime", "60s")),
        props.getProperty("kafkaServers", ""),
        props.getProperty("kafkaTopic", "tweets"),
        props.getProperty("searchTerms", ""),
        props.getProperty("languages", ""),
        props.getProperty("users", ""),
        props.getProperty("elasticUrl", "http://localhost:9200"),
        twitterProps))
    } else {
      None
    }

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