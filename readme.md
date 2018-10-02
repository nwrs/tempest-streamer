# Tempest Streamer

Real-time Twitter analytic dashboards and mapping with Apache Flink, Elasticsearch and Kibana.

![Example Output](https://github.com/nwrs/tempest-streamer/blob/master/docs/images/screenshots/kibana-screenshot.jpg)

![Example Output](https://github.com/nwrs/tempest-streamer/blob/master/docs/images/screenshots/uk-mapping.jpg)


### Features

* Track groups of users or hashtags and keywords.
* Real-time reporting of top hashtags, topics, influencers, retweets, links etc...
* User profile topics/hashtags/bigrams, demographic breakdown.
* Real-time mapping (including parsing of user profile location text to geo coords).
* Run standalone or within Flink cluster.
* Directly connects to Twitter API or can receive Avro encoded tweets over Kafka.
* Compatible with Amazon EC2 (Elasticsearch Service / EMR).

### Requirements

* Scala 2.11, Maven 3.x
* Elasticsearch / Kibana 6.2.x
* Flink 1.6.x (when running within a Flink cluster)
* Kafka 10.1 (optional)
* A Twitter developer account, register [here](https://developer.twitter.com/en/apply/user)
* Register a Twitter app to acquire consumer keys/tokens [here](https://apps.twitter.com)


### Install and run from release

Install:

```
$ curl -L -O https://github.com/nwrs/tempest-streamer/releases/download/release-1.0/tempest-streamer-1.0.0.tar.gz
$ tar -xvf tempest-streamer-1.0.0.tar.gz
$ cd tempest-streamer-1.0.0
```

Create configuration file with search terms and Twitter credentials:

```
$ cp conf/example.properies myConfig.properties
$ vi myConfig.properties
```

Example configuration file:
```
# Configuration example for a direct connection to Twitter
elasticUrl=http://localhost:9200
tweetSource=direct
searchTerms=barcelona,#fcb,#fcbarcelona,#CampNou
languages=en
# Add credentials here
twitter-source.consumerKey=YOUR_KEY
twitter-source.consumerSecret=YOUR_SECRET
twitter-source.token=YOUR_TOKEN
twitter-source.tokenSecret=YOUR_TOKEN_SECRET
```

Run:

````
$ ./tempest-start.sh --configFile myConfig.properties

2018-10-02 00:06:00 INFO  MainApp$:38 - Starting Tempest Tweet Streamer...
2018-10-02 00:06:00 INFO  MainApp$:31 - timeWindow = 1m
2018-10-02 00:06:00 INFO  MainApp$:31 - configFile = myConfig.properties
2018-10-02 00:06:00 INFO  MainApp$:31 - twitter-source.tokenSecret = xxxxxxxxxxxxx
2018-10-02 00:06:00 INFO  MainApp$:31 - languages = en
2018-10-02 00:06:00 INFO  MainApp$:31 - searchTerms = barcelona,#fcb,#fcbarcelona,#CampNou
2018-10-02 00:06:00 INFO  MainApp$:31 - twitter-source.token = xxxxxxxxxxxxx
2018-10-02 00:06:00 INFO  MainApp$:31 - twitter-source.consumerSecret = xxxxxxxxxxxxx
2018-10-02 00:06:00 INFO  MainApp$:31 - twitter-source.consumerKey = xxxxxxxxxxxxx
2018-10-02 00:06:00 INFO  MainApp$:31 - elasticUrl = http://localhost:9200
2018-10-02 00:06:03 INFO  ElasticUtils:55 - Elasticsearch cluster status: yellow
2018-10-02 00:06:03 INFO  ElasticUtils:74 - Ensuring Elasticsearch indexes
2018-10-02 00:06:03 INFO  MainApp$:60 - Streams will window every 60s with parallelism of 1
2018-10-02 00:06:03 INFO  StreamFactory$:44 - Creating stream from direct Twitter connection.
2018-10-02 00:06:03 INFO  MainApp$:81 - Adding result to stream: Hashtags - Windowed count of hashtags
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: TweetCount - Windowed count of tweet by type
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: ProfileTopics - Windowed count of user profile topics
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: ProfileBigrams - Windowed count of user profile bigrams
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: Influencers - Windowed top 10 users by followers
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: Gender - Windowed breakdown of user gender
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: Source - Windowed breakdown of tweet app/device/platform source
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: GeoLocation - Windowed geo locations parsed from user profile location text
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: Retweets - Windowed count of top retweets
2018-10-02 00:06:04 INFO  MainApp$:81 - Adding result to stream: Links - Windowed count of tweeted weblinks
2018-10-02 00:06:07 INFO  BasicClient:95 - New connection executed: flink-twitter-source, endpoint: /1.1/statuses/filter.json?delimited=length&stall_warnings=true
2018-10-02 00:06:07 INFO  ClientBase:173 - flink-twitter-source Establishing a connection
2018-10-02 00:06:08 INFO  ClientBase:240 - flink-twitter-source Processing connection data
````

Follow instructions to configure Kibana [here](https://github.com/nwrs/tempest-streamer/wiki/Configuring-Kibana).


### Build

```
$ git clone https://github.com/nwrs/tempest-streamer.git
$ cd tempest-streamer
$ mvn clean install
```


