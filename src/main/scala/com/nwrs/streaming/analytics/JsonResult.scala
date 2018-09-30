package com.nwrs.streaming.analytics

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.codehaus.jettison.json.JSONObject

trait JsonResult[T] {
  def +(other:T):T
  def toJson(): String = write(this)(DefaultFormats)
  def total(): Int = 0
  def key():String = ""
}

object JsonResult {
  def cleanAndQuote(txt: String) = JSONObject.quote(txt.replaceAll("[^\\p{ASCII}]", "")) //TODO is this still required when using elastic API?
}

