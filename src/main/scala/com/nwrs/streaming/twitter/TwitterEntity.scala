package com.nwrs.streaming.twitter

abstract class TwitterEntity

object TwitterEntity {
  val limitationNoticeRegex ="""\{"limit":\{"track":([0-9]+),"timestamp_ms":"([0-9]+)"\}\}""".r
  val tweetPrefix="{\"cre"
  def fromJson(json:String):Option[TwitterEntity] = json match {
    case j if j.startsWith(tweetPrefix) => Tweet.fromJson(j)
    case limitationNoticeRegex(limit,timestamp) => Some(LimitationNotice(limit.toInt,timestamp.toLong))
    case _ => None
  }
}
