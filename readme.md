# Tempest Streamer

Real-time Twitter analytic dashboards and mapping with Apache Flink, Elasticsearch and Kibana.

![Example Output](https://github.com/nwrs/tempest-streamer/blob/master/docs/images/screenshots/kibana-screenshot.jpg)
![Example Output](https://github.com/nwrs/tempest-streamer/blob/master/docs/images/screenshots/uk-mapping.jpg)
![Example Output](https://github.com/nwrs/tempest-streamer/blob/master/docs/images/screenshots/user-profiles.jpg)

### Features

Track multiple Twitter hashtags, keywords, users and more in real-time dashboards:

* Track groups of users or keywords, search terms and hashtags.
* Real-time reporting of top hashtags, topics, influencers, retweets, links etc...
* User profile analysis: profile topics/hashtags/bigram counts, demographic breakdown.
* Real-time mapping (including parsing of user profile location text to geo coords)
* Runs standalone or within Flink cluster
* Connect to Twitter API or optionally receive Avro encoded tweets over Kafka
* Compatible with Amazon EC2 (Elasticsearch Service / EMR)

### Requirements

* Scala 2.11
* Maven 3.x
* Elasticsearch / Kibana 6.2.x
* Flink 1.6.x (when running within a Flink cluster)
* Kafka 10.1 (optional)
* Twitter developer account

