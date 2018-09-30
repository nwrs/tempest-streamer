package com.nwrs.streaming.twitter

import java.util.Date
import com.nwrs.streaming.parsing.Gender.Gender
import com.nwrs.streaming.parsing.{GenderParser, GeoLocation, GeoParser}
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import twitter4j.TwitterObjectFactory._

import scala.collection.JavaConverters._

// Tweet case class, analytic stream operations work on instances of this
case class Tweet(accountName:String="",
                 screenName:String="",
                 text:String="",
                 imgUrl:String="",
                 date:Long=new Date().getTime,
                 retweet:Boolean=false,
                 followers:Long=0,
                 userId:Long=0,
                 verified:Boolean=false,
                 profileText:String="",
                 profileLocation:String="",
                 inReplyTo:Long=0,
                 hasLinks:Boolean=false,
                 links:String="",
                 hasHashtags:Boolean=false,
                 hashtags:Array[String]=Array[String](),
                 retweetAccountName:String="",
                 retweetScreenName:String="",
                 retweetNumRetweets:Long=0,
                 retweetFollowers:Long=0,
                 retweetImgUrl:String="",
                 tweetType:String="",
                 source:String="",
                 id:Long=0,
                 retweetId:Long=0,
                 resolvedProfileLocation:String="",
                 profileTown: String="",
                 profileArea: String="",
                 profileRegion: String="",
                 profileCountry: String="",
                 locationGeo:String="",
                 locationAccuracy: Int=0,
                 iso31662: String="",
                 gender:String="") extends TwitterEntity

object Tweet {

  lazy val log:Logger = LoggerFactory.getLogger(Tweet.getClass)

  /**
    * Factory method to create from an Avro record (as received by Kafka)
    * @param record Avro record
    * @return Populated Tweet
    */
  def fromAvro(record: GenericRecord): Tweet = {
    import AvroImplicits._

    try {
      val inReplyTo = record.getAs[Long]("inReplyTo")
      val retweet = record.getAs[Boolean]("retweet")

      Tweet(accountName = record.getAsString("accountName"),
        screenName = record.getAsString("screenName"),
        imgUrl = record.getAsString(("imgUrl")),
        text = record.getAsString(("text")),
        date = record.getAs[Long]("date"),
        retweet = retweet,
        followers = record.getAs[Long]("followers"),
        userId = record.getAs[Long]("userId"),
        verified = record.getAs[Boolean]("verified"),
        profileText = record.getAsString("profileText"),
        profileLocation = record.getAsString("profileLocation"),
        inReplyTo = inReplyTo,
        hasLinks = record.getAs[Boolean]("hasLinks"),
        links = record.getAsString("links"),
        hasHashtags = record.getAs[Boolean]("hasHashtags"),
        hashtags = record.getAs[java.util.List[org.apache.avro.util.Utf8]]("hashtags") match {
          case null => Array[String]()
          case h => h.asScala.map(_.toString).toArray
        },
        retweetAccountName = record.getAsString("retweetAccountName"),
        retweetScreenName = record.getAsString("retweetScreenName"),
        retweetNumRetweets = record.getAs[Long]("retweetNumRetweets"),
        retweetFollowers = record.getAs[Long]("retweetFollowers"),
        retweetImgUrl = record.getAsString("retweetImgUrl"),
        tweetType = if (retweet) "Retweet" else if (inReplyTo > 0) "Reply" else "Original Tweet",
        source = record.getAsString("source"),
        id = record.getAs[Long]("id"),
        retweetId = record.getAs[Long]("retweetId"))
        // TODO - populate geo location fields
    } catch {
      case t:Throwable => {
        log.error("Failed to create tweet from Avro: ", t)
        throw t
      }
    }
  }


  /**
    * Create a Tweet object from Twitter JSON (when using direct Twitter API connection)
    * TODO Creating via a Twitter4J Status object for convenience, remove this indirection/dependency
    * @param twitterJson
    * @return
    */
  def fromJson(twitterJson:String):Option[Tweet] = {
      try {
        val t = createStatus(twitterJson)

        // extract main text
        val isRetweet = t.isRetweet
        val text = if (isRetweet)
          t.getRetweetedStatus.getText
        else
          t.getText

        // link entities
        // TODO - Should these entities come from retweeted status ?
        val entities = t.getURLEntities
        val hasLinks = (entities != null & entities.length > 0)
        val links = if (hasLinks)
          entities.toSeq.map(_.getExpandedURL).mkString(" ")
        else
          ""

        // hashtag entities
        // TODO - Should these entities come from retweeted status ?
        val htEntities = t.getHashtagEntities
        val hasHashtags = (htEntities != null & htEntities.length > 0)
        val hashtags = if (hasHashtags)
          htEntities.toSeq.map(_.getText).toArray
        else
          Array[String]()

        val u = t.getUser
        val geo:Option[GeoLocation] = GeoParser.parse(u.getLocation)
        val gender:Option[Gender] = GenderParser.parseGenderFromFullName(u.getName)
        Some(Tweet(accountName = u.getName,
          screenName = u.getScreenName,
          text = if (text !=null) text else "",
          imgUrl = if (u.getProfileImageURL !=null) u.getProfileImageURL else "",
          date = t.getCreatedAt.getTime,
          retweet = isRetweet,
          followers = u.getFollowersCount,
          userId = u.getId,
          verified = u.isVerified,
          profileText = if (u.getDescription !=null) u.getDescription else "",
          profileLocation = if (u.getLocation !=null) u.getLocation else "",
          inReplyTo = t.getInReplyToStatusId,
          hasLinks = hasLinks,
          links = links,
          hasHashtags = hasHashtags,
          hashtags = hashtags,
          retweetAccountName = if (isRetweet) t.getRetweetedStatus.getUser.getName else "",
          retweetScreenName = if (isRetweet) t.getRetweetedStatus.getUser.getScreenName else "",
          retweetNumRetweets = if (isRetweet) t.getRetweetedStatus.getRetweetCount else 0,
          retweetFollowers = if (isRetweet) t.getRetweetedStatus.getUser.getFavouritesCount else 0,
          retweetImgUrl = if (isRetweet) t.getRetweetedStatus.getUser.getProfileImageURL else "",
          tweetType = if (isRetweet) "Retweet" else if (t.getInReplyToStatusId > 0) "Reply" else "Original Tweet",
          source = if (t.getSource!=null) t.getSource else "",
          id = t.getId,
          retweetId = if (isRetweet) t.getRetweetedStatus.getId else 0,
          resolvedProfileLocation = if (geo.nonEmpty) geo.get.location else "",
          profileTown = if (geo.nonEmpty) geo.get.town else "",
          profileArea = if (geo.nonEmpty) geo.get.area else "",
          profileRegion = if (geo.nonEmpty) geo.get.region else "",
          profileCountry = if (geo.nonEmpty) geo.get.country else "",
          locationGeo = if (geo.nonEmpty) geo.get.locationGeo else "",
          locationAccuracy = if (geo.nonEmpty) geo.get.accuracy else 0,
          iso31662 = if (geo.nonEmpty) geo.get.iso31662 else "",
          gender = if (gender.nonEmpty) gender.get.toString else "")
        )
      } catch {
        case t: Throwable => {
          log.error("Failed to create tweet from JSON: ", t)
          return None
        }
      }
  }

  // Avro deserialization
  private val avroSchema =  new Schema.Parser().parse(Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("avro/tweet.avsc")).mkString)
  private val recordInjection = GenericAvroCodecs.toBinary(avroSchema)

  class TweetDeserializationSchema extends AbstractDeserializationSchema[Tweet] {
    override def deserialize(bytes: Array[Byte]): Tweet = {
      val record: GenericRecord = recordInjection.invert(bytes).get
      Tweet.fromAvro(record)
    }
  }

}
