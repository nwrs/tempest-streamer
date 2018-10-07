package com.nwrs.streaming.analytics

import java.util

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * A simple collection that retains only the top n elements by natural sort order.
  * Not thread safe
  * @param limit The max number of items
  * @tparam K Type that defines the sort
  * @tparam V Value type
  */
class BoundSortedCollection[K <% Ordered[K],V](limit:Int) extends Iterable[V] {
  private val map = new util.TreeMap[K,V]() // TODO replace with new scala mutable TreeMap when upgrading to 2.12

  def +=(k:K,v:V) = {
    if (map.size < limit || k > map.firstEntry().getKey) {
      map.put(k,v)
      if (map.size() > limit) map.remove(map.firstEntry().getKey)
    }
    this
  }

  def ++=(vals:Iterable[(K,V)]) = {
    vals.foreach(e => +=(e._1,e._2))
    this
  }

  def iterator(): Iterator[V] = {
    map.values().asScala.iterator
  }
}

/**
  * Flink processor that retains only the Top n elements of a stream time window by natural sort order
  * @param extractor Extractor that returns the value to rank by
  * @param n Number of elements to retain
  * @tparam T Type of the element
  * @tparam S Type of the value to rank by
  */
class TopNReducer[S <% Ordered[S], T <: JsonResult[_]](extractor:T => S, n:Int, reverseOrder:Boolean = false) extends ProcessAllWindowFunction[T, T, TimeWindow] {
  override def process(context: Context, elements: Iterable[T], out: Collector[T]): Unit = {
    val topN = new BoundSortedCollection[S, T](n)
    topN ++= elements.map(e => (extractor.apply(e), e))
    topN.iterator.foreach(out.collect(_))
  }
}

