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
* Twitter developer account


### Install and run from release

```
curl -L -O https://github.com/nwrs/tempest-streamer/releases/download/release-1.0/tempest-streamer-1.0.0.tar.gz
tar -xvf tempest-streamer-1.0.0.tar.gz
cd tempest-streamer-1.0.0
./tempest-start.sh
```

### Build

```
git clone https://github.com/nwrs/tempest-streamer.git
cd tempest-streamer
mvn clean install
```


