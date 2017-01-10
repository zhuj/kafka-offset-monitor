package com.quantifind.kafka.core

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetGetterArgs

import java.nio.ByteBuffer
import java.util.{Arrays, Properties}

import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.coordinator._
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, duration}


/**
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter(theZkUtils: ZkUtils, args: OffsetGetterArgs) extends OffsetGetter {

  import KafkaOffsetGetter._

  override val zkUtils = theZkUtils

  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {

    val topicAndPartition = TopicAndPartition(topic, pid)

    committedOffsetMap.get(GroupTopicPartition(group, topicAndPartition)) map { offsetMetaData =>

      // BIT O'HACK to deal with timing:
      // Due to thread and processing timing, it is possible that the value we have for the topicPartition's
      // logEndOffset has not yet been updated to match the value on the broker.  When this happens, report the
      // topicPartition's logEndOffset as "committedOffset - lag", as the topicPartition's logEndOffset is *AT LEAST*
      // this value
      val logEndOffset: Option[Long] = topicPartitionOffsetsMap.get(topicAndPartition)
      val committedOffset: Long = offsetMetaData.offset
      val lag: Long = logEndOffset.get - committedOffset
      val logEndOffsetReported: Long = if (lag < 0) committedOffset - lag else logEndOffset.get

      val client: Option[ClientGroup] = Option(clients.filter(c => (c.group == group && c.topicPartitions.contains(topicAndPartition))).head)
      val clientString: Option[String] = if (client.isDefined) Option(client.get.clientId + client.get.clientHost) else Option("NA")

      OffsetInfo(group = group,
        topic = topic,
        partition = pid,
        offset = committedOffset,
        logSize = logEndOffsetReported,
        owner = clientString,
        creation = Time.fromMilliseconds(offsetMetaData.commitTimestamp),
        modified = Time.fromMilliseconds(offsetMetaData.expireTimestamp))
    }
  }

  override def getGroups: Seq[String] = {
    topicAndGroups.groupBy(_.group).keySet.toSeq.sorted
  }

  override def getTopicList(group: String): List[String] = {
    topicAndGroups.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
  }

  override def getTopicMap: Map[String, scala.Seq[String]] = {
    topicAndGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    getTopicMap
  }
}

object KafkaOffsetGetter extends Logging {

  val committedOffsetMap: mutable.Map[GroupTopicPartition, OffsetAndMetadata] = mutable.HashMap()
  val topicPartitionOffsetsMap: mutable.Map[TopicAndPartition, Long] = mutable.HashMap()
  val topicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()
  val clients: mutable.Set[ClientGroup] = mutable.HashSet()

  var tpOffsetGetterConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null;

  case class TopicAndGroup(topic: String, group: String)

  case class ClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicAndPartition])

  private def createNewKafkaConsumer(args: OffsetGetterArgs, group: String): KafkaConsumer[Array[Byte], Array[Byte]] = {

    val props: Properties = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("security.protocol", args.kafkaSecurityProtocol)

    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  private def createNewAdminClient(args: OffsetGetterArgs): AdminClient = {

    val sleepAfterFailedAdminClientConnect: Int = 60000
    var hasGoodAdminClient: Boolean = false
    var adminClient: AdminClient = null

    while (!hasGoodAdminClient) {

      try {

        info("Creating new Kafka AdminClient to get consumer and group info.");
        val props: Properties = new Properties
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
        props.put("security.protocol", args.kafkaSecurityProtocol)

        adminClient = AdminClient.create(props)
        hasGoodAdminClient = true
      }

      catch {

        case e: Throwable =>
          adminClient = null
          hasGoodAdminClient = false
          val errorMsg = "Error creating an AdminClient.  Will attempt to re-create in %d seconds".format(sleepAfterFailedAdminClientConnect)
          error(errorMsg, e)
          Thread.sleep(sleepAfterFailedAdminClientConnect)
      }
    }

    adminClient
  }

  def startCommittedOffsetListener(args: OffsetGetterArgs) = {

    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null;

    if (null == offsetConsumer) {

      logger.info("Creating new Kafka Client to get consumer group committed offsets")

      val group: String = "kafka-offset-getter-client-" + System.currentTimeMillis
      val consumerOffsetTopic = "__consumer_offsets"

      offsetConsumer = createNewKafkaConsumer(args, group)
      offsetConsumer.subscribe(Arrays.asList(consumerOffsetTopic))
    }

    Future {
      try {

        while (true) {

          val records: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(100)

          if (0 != records.count) {

            val iter = records.iterator()

            while (iter.hasNext()) {

              val record: ConsumerRecord[Array[Byte], Array[Byte]] = iter.next()
              val baseKey: BaseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key))
              baseKey match {

                case b: OffsetKey =>
                  val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value))
                  val gtp: GroupTopicPartition = b.key

                  val existingCommittedOffsetMap: Option[OffsetAndMetadata] = committedOffsetMap.get(gtp)

                  // Update only if the new message brings a change in offset
                  if (!existingCommittedOffsetMap.isDefined || existingCommittedOffsetMap.get.offset != offsetAndMetadata.offset) {

                    val group: String = gtp.group
                    val topic: String = gtp.topicPartition.topic
                    val partition: Long = gtp.topicPartition.partition
                    val offset: Long = offsetAndMetadata.offset
                    logger.info(s"Updating committed offset: g:$group,t:$topic,p:$partition: $offset")

                    committedOffsetMap += (gtp -> offsetAndMetadata)
                  }

                case _ => // do nothing with these other messages
              }
            }
          }
        }

      } catch {

        case e: Throwable =>
          val errorMsg = String.format("The Kafka Client reading consumer group committed offsets has thrown an unhandled exception.")
          fatal(errorMsg, e)
          System.exit(1)
      }
    }
  }

  // Retrieve ConsumerGroup, Consumer, Topic, and Partition information
  def startAdminClient(args: OffsetGetterArgs) = {

    val sleepOnException: Int = 60000
    val sleepOnDataRetrieval: Int = 10000

    Future {

      while (true) {

        var adminClient: AdminClient = null

        try {

          adminClient = createNewAdminClient(args)
          var hasError: Boolean = false

          while (!hasError) {

            lazy val f = Future {

              try {

                def groupOverviews = adminClient.listAllConsumerGroupsFlattened()

                groupOverviews.foreach((groupOverview: GroupOverview) => {

                  val groupId = groupOverview.groupId;
                  val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

                  consumerGroupSummary.foreach((consumerSummary) => {

                    val clientId = consumerSummary.clientId
                    val clientHost = consumerSummary.clientHost

                    val topicPartitions: List[TopicPartition] = consumerSummary.assignment
                    var topicAndPartitions: mutable.Set[TopicAndPartition] = mutable.HashSet()

                    topicPartitions.foreach((topicPartition) => {

                      val topic = topicPartition.topic()
                      val partitionId = topicPartition.partition()
                      val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partitionId)

                      topicAndGroups += TopicAndGroup(topic, groupId)
                      topicAndPartitions += topicAndPartition
                    })

                    clients += ClientGroup(groupId, clientId, clientHost, topicAndPartitions)
                  })
                })
              }

              catch {

                case e: Throwable =>

                  hasError = true
                  if (null != adminClient) {
                    adminClient.close()
                  }
                  val errorMsg: String = "Kafka AdminClient polling aborted due to unexpected exception."
                  error(errorMsg, e)
              }
            }

            Await.result(f, duration.pairIntToDuration(30, duration.SECONDS))

            if (hasError) {

              Thread.sleep(sleepOnException)
            } else {

              Thread.sleep(sleepOnDataRetrieval)
            }
          }
        }

        catch {

          case ex: java.util.concurrent.TimeoutException => {
            error("The AdminClient timed out.  It will be closed and restarted.")
            if (null != adminClient) {
              adminClient.close()
            }
          }
        }
      }
    }
  }

  def startTopicPartitionOffsetGetter(args: OffsetGetterArgs) = {

    Future {

      try {

        while (true) {

          // Get list of distinct TopicAndPartitions
          val distinctTopicPartitions: List[TopicAndPartition] = clients.flatMap(c => c.topicPartitions).toList.distinct

          if (null == tpOffsetGetterConsumer) {

            val group: String = "KafkaOffsetMonitor-TopicPartOffsetGetter-" + System.currentTimeMillis()
            tpOffsetGetterConsumer = createNewKafkaConsumer(args, group)

            // Do I need to assign to each topicPartition?
          }

          // Iterate over each distinct TopicPartition
          distinctTopicPartitions.foreach(topicAndPartition => {

            // Get the LogEndOffset for the TopicPartition
            val topicPartition: TopicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
            tpOffsetGetterConsumer.assign(Arrays.asList(topicPartition))
            tpOffsetGetterConsumer.seekToEnd(topicPartition)
            val logEndOffset: Long = tpOffsetGetterConsumer.position(topicPartition)

            // Update the TopicPartition map with the current LogEndOffset if it exists, else add a new map
            if (topicPartitionOffsetsMap.contains(topicAndPartition)) {
              topicPartitionOffsetsMap.update(topicAndPartition, logEndOffset)
            }
            else {
              topicPartitionOffsetsMap += (topicAndPartition -> logEndOffset)
            }
          })
        }

      } catch {

        case e: Throwable =>
          val errorMsg = String.format("The Kafka Client reading topic/partition LogEndOffsets has thrown an unhandled exception.")
          fatal(errorMsg, e)
          System.exit(1)
      }
    }

  }
}