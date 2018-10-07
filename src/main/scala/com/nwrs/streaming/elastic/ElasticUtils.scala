package com.nwrs.streaming.elastic

import java.net.URL
import java.util.Properties
import com.nwrs.streaming.analytics.JsonResult
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.Http
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import net.liftweb.json._
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

import scala.io.Source

case class ElasticClusterHealth(cluster_name: String,
                                status : String,
                                timed_out : Boolean,
                                number_of_nodes: Int,
                                number_of_data_nodes : Int,
                                active_primary_shards : Int,
                                active_shards : Int,
                                relocating_shards : Int,
                                initializing_shards : Int,
                                unassigned_shards : Int,
                                delayed_unassigned_shards: Int,
                                number_of_pending_tasks : Int,
                                number_of_in_flight_fetch: Int,
                                task_max_waiting_in_queue_millis: Int,
                                active_shards_percent_as_number: Float)

class ElasticUtils(path:String) {
  val log = LoggerFactory.getLogger(this.getClass)

  val url = new URL(path)
  lazy val client =  ClientBuilder()
      .hosts(path.replace(url.getProtocol + "://", ""))
      .hostConnectionLimit(10)
      .tcpConnectTimeout(Duration.fromSeconds(10))
      .retries(2)
      .stack(if (url.getProtocol=="https") Http.client.withTls(url.getHost) else Http.client)
      .build()

  implicit val formats = Serialization.formats(NoTypeHints)

  def isClusterUp():Boolean = {
    val health = getClusterHealth()
    if (health.nonEmpty) {
      log.info(s"Elasticsearch cluster status: ${health.get.status}")
      (health.get.status.equals("yellow") || health.get.status.equals("green"))
    } else {
      false
    }
  }

  def getClusterHealth():Option[ElasticClusterHealth] = {
    val healthResponse = Await.result(client(RequestBuilder.create().url(path+"/_cluster/health").buildGet()))
    if (healthResponse.statusCode == 200) {
      Some(parse(healthResponse.getContentString()).extract[ElasticClusterHealth])
    } else {
      None
    }
  }

  def loadIndexFile(index:String):String = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(s"elastic/indexes/$index.json")).mkString

  def provisionIndices() = {
    log.info("Ensuring Elasticsearch indexes")
    val indexResults = ElasticUtils.indices.map { i => client(RequestBuilder.create().url(path+s"/$i").buildGet())
        .filter(_.statusCode == 404) // 404 = index not found, need to create
        .flatMap(_ => { // PUT to create index
          log.info(s"Creating index: $i")
          client(RequestBuilder.create().url(path+s"/$i").buildPut(Buf.Empty))
        })
        .flatMap( _ => { // Create index mappings
          log.info(s"Creating index mappings: $i")
          client(RequestBuilder
            .create()
            .url(path+s"/$i/_mappings/${i.replaceFirst("-idx","")+"-timeline"}")
            .addHeader("Content-Type","application/json")
            .buildPut(Buf.Utf8(loadIndexFile(i))))
        })
    }
    Await.all(indexResults :_*)
  }

  def ensureElasticSearchCluster() = {
    isClusterUp() match {
      case true => provisionIndices()
      case false => throw new Exception(s"Elasticsearch cluster not available at $path")
    }
  }
}


object ElasticUtils {

  val indices = Seq("count-idx",
    "gender-idx",
    "geo-idx",
    "hashtags-idx",
    "links-idx",
    "profile-bigrams-idx",
    "profile-topics-idx",
    "retweets-idx",
    "influencers-idx")

  def createSink[T <: JsonResult[_]](idx:String, mapping:String, elasticUrl:String) = {
    val httpHosts = elasticUrl.split(",").map(HttpHost.create(_)).toSeq
    val sinkFunc = new ElasticsearchSinkFunction[T] {
      override def process(cc: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(Requests.indexRequest()
          .index(idx)
          .`type`(mapping)
          .source(cc.toJson, XContentType.JSON))
      }
    }
    import collection.JavaConverters._
    val esSinkBuilder = new ElasticsearchSink.Builder[T](httpHosts.asJava,sinkFunc)
    esSinkBuilder.setBulkFlushMaxActions(50)
    esSinkBuilder.setBulkFlushInterval(30000)
    esSinkBuilder.setBulkFlushBackoffRetries(5)
    esSinkBuilder.setBulkFlushBackoffDelay(2000)
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        // TODO Additional rest client args go here - authentication headers for secure connections etc...
      }
    })
    esSinkBuilder.build()
  }
}

