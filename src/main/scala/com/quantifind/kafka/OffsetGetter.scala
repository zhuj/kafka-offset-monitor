package com.quantifind.kafka

import scala.collection._

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, Logging, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import com.twitter.util.Time
import org.apache.zookeeper.data.Stat
import scala.util.control.NonFatal

/**
 * a nicer version of kafka's ConsumerOffsetChecker tool
 * User: pierre
 * Date: 1/22/14
 */

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])
case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])
case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

class OffsetGetter(zkClient: ZkClient) extends Logging {

  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  private def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case (None, _) =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        error("Could not parse broker info", t)
        None
    }
  }

  private def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      val (offset, stat: Stat) = ZkUtils.readData(zkClient, s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val (owner, _) = ZkUtils.readDataMaybeNull(zkClient, s"${ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      ZkUtils.getLeaderForPartition(zkClient, topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = TopicAndPartition(topic, pid)
              val request =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

              OffsetInfo(group = group,
                topic = topic,
                partition = pid,
                offset = offset.toLong,
                logSize = logSize,
                owner = owner,
                creation = Time.fromMilliseconds(stat.getCtime),
                modified = Time.fromMilliseconds(stat.getMtime))
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  private def processTopic(group: String, topic: String): Seq[OffsetInfo] = {
    val pidMap = ZkUtils.getPartitionsForTopics(zkClient, Seq(topic))
    for {
      partitions <- pidMap.get(topic).toSeq
      pid <- partitions.sorted
      info <- processPartition(group, topic, pid)
    } yield info
  }

  private def brokerInfo(): Iterable[BrokerInfo] = {
    for {
      (bid, consumerOpt) <- consumerMap
      consumer <- consumerOpt
    } yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
  }

  private def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {
    val topicList = if (topics.isEmpty) {
      getTopicList(group)
    } else {
      topics
    }

    topicList.sorted.flatMap(processTopic(group, _))
  }

  def getTopicList(group: String): List[String] = {
    try {
      ZkUtils.getChildren(zkClient, s"${ZkUtils.ConsumersPath}/$group/offsets").toList
    } catch {
      case _: ZkNoNodeException => List()
    }
  }

  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val off = offsetInfo(group, topics)
    val brok = brokerInfo()
    KafkaInfo(
      name = group,
      brokers = brok.toSeq,
      offsets = off
    )
  }

  def getGroups: Seq[String] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  /**
   * Returns details for a given topic such as the consumers pulling off of it
   */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }).toSeq)
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

  def getTopics: Seq[String] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.BrokerTopicsPath).sortWith(_ < _)
    } catch {
      case NonFatal(t) =>
        error(s"could not get topics because of ${t.getMessage}", t)
        Seq()

    }
  }


  /**
   * Returns a map of active topics -> list of consumers from zookeeper, ones that have IDS attached to them
   */
  def getActiveTopicMap: Map[String, Seq[String]] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).flatMap {
        group =>
          try {
            ZkUtils.getConsumersPerTopic(zkClient, group).keySet.map {
              key =>
                key -> group
            }
          } catch {
            case NonFatal(t) =>
              error(s"could not get consumers for group $group", t)
              Seq()
          }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  /**
   * Returns a map of topics -> list of consumers, including non-active
   */
  def getTopicMap: Map[String, Seq[String]] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).flatMap {
        group => {
          getTopicList(group).map(topic => topic -> group)
        }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def getClusterViz: Node = {
    val clusterNodes = ZkUtils.getAllBrokersInCluster(zkClient).map((broker) => {
      Node(broker.getConnectionString(), Seq())
    })
    Node("KafkaCluster", clusterNodes)
  }

  def close() {
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
        case Some(consumer) => consumer.close()
        case None => // ignore
      }
    }
  }

}

object OffsetGetter {

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

}
