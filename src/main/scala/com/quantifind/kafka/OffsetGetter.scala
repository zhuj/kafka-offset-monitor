package com.quantifind.kafka

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import com.quantifind.kafka.core._
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.twitter.util.Time
import kafka.consumer.SimpleConsumer
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection._


case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])

case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

trait OffsetGetter extends Logging {

  val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  //  kind of interface methods

  def getTopicList(group: String): List[String]

  def getGroups: Seq[String]

  def getActiveGroups: Seq[String]

  def getTopicMap: Map[String, Seq[String]]

  def getTopicsAndLogEndOffsets: Seq[TopicLogEndOffsetInfo]

  def getActiveTopicMap: Map[String, Seq[String]]

  def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo]

  protected def processTopic(group: String, topic: String): Seq[OffsetInfo]

  protected def brokerInfo(): Iterable[BrokerInfo] = {
    for {
      (bid, consumerOpt) <- consumerMap
      consumer <- consumerOpt
    } yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
  }

  protected def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {
    val topicList = if (topics.isEmpty) {
      getTopicList(group)
    } else {
      topics
    }
    topicList.sorted.flatMap(processTopic(group, _))
  }


  // get information about a consumer group and the topics it consumes
  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val off = offsetInfo(group, topics)
    val brok = brokerInfo()
    KafkaInfo(
      name = group,
      brokers = brok.toSeq,
      offsets = off
    )
  }

  // get list of all topics
  def getTopics: Seq[String]

  def getClusterViz: Node

  /**
    * Returns details for a given topic such as the consumers pulling off of it
    */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(
        topicMap(topic).map(
          consumer => {
            ConsumerDetail(consumer.toString)
          }
        )
      )
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def mapConsumerDetails(consumers: Seq[String]): Seq[ConsumerDetail] =
    consumers.map(consumer => ConsumerDetail(consumer.toString))

  /**
    * Returns details for a given topic such as the active consumers pulling off of it
    * and for each of the active consumers it will return the consumer data
    */
  def getTopicAndConsumersDetail(topic: String): TopicAndConsumersDetailsWrapper = {
    val topicMap = getTopicMap
    val activeTopicMap = getActiveTopicMap

    val activeConsumers = if (activeTopicMap.contains(topic)) {
      mapConsumersToKafkaInfo(activeTopicMap(topic), topic)
    } else {
      Seq()
    }

    val inactiveConsumers = if (!activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic), topic)
    } else if (activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic).diff(activeTopicMap(topic)), topic)
    } else {
      Seq()
    }

    TopicAndConsumersDetailsWrapper(TopicAndConsumersDetails(activeConsumers, inactiveConsumers))
  }

  def mapConsumersToKafkaInfo(consumers: Seq[String], topic: String): Seq[KafkaInfo] =
    consumers.map(getInfo(_, Seq(topic)))


  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def getConsumerGroupStatus: String

  def close() {
    // TODO: What is going on here?  This code is broken
    /*
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
      case Some(consumer) => consumer.close()
      case None => // ignore
      }
    }
    */
  }
}

object OffsetGetter extends Logging {

  case class KafkaInfo(name: String, brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
    topic: String,
    partition: Int,
    offset: Long,
    logSize: Long,
    owner: Option[String],
    creation: Time,
    modified: Time) {
    val lag = logSize - offset
  }

  val kafkaOffsetListenerStarted: AtomicBoolean = new AtomicBoolean(false)
  var newKafkaConsumer: KafkaConsumer[String, String] = null

  def getInstance(args: OffsetGetterArgs): OffsetGetter = {

    if (kafkaOffsetListenerStarted.compareAndSet(false, true)) {
      val daemon = new ThreadFactoryBuilder().setDaemon(true).build()

      Executors.newSingleThreadExecutor(daemon).submit(new Runnable() {
        override def run(): Unit = KafkaOffsetGetter.startAdminClient(args)
      })
      Thread.sleep(1000)

      Executors.newSingleThreadExecutor().submit(new Runnable() {
        override def run(): Unit = KafkaOffsetGetter.startLogEndOffsetGetter(args)
      })
      Thread.sleep(1000)

      Executors.newSingleThreadExecutor().submit(new Runnable() {
        override def run(): Unit = KafkaOffsetGetter.startCommittedOffsetListener(args)
      })
      Thread.sleep(1000)
    }

    new KafkaOffsetGetter(args)
  }
}
