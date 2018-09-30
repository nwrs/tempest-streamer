package com.nwrs.streaming.elastic

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticRESTTest extends FlatSpec  {

  behavior of "A Elasticsearch REST interface"

  ignore should "successfully check the running status of the Elasticsearch cluster" in {
    val utils = new ElasticUtils("https://localhost:9200")
    utils.ensureElasticSearchCluster()
  }




}
