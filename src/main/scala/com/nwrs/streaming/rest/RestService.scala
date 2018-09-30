package com.nwrs.streaming.rest

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.Await
import io.finch._
import io.finch.syntax._
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.slf4j.LoggerFactory


object RestService {

  val LOG = LoggerFactory.getLogger(this.getClass)
  implicit val formats = Serialization.formats(NoTypeHints)

  private case class Status(health:String,  msg:String)

  val status: Endpoint[String] = get("status") {
    LOG.info("[GET] /status")
    // TODO enquire status
    Ok(write(Status("OK", "All is good")))
  }

  // TODO: Stats, stop/start/restart stream with new search terms

  def startAndAwait(port:Int) = {
    val api: Service[Request, Response] = (status).toServiceAs[Text.Plain]
    Await.ready(Http.server.serve(s":$port", api))
  }

}
